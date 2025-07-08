"""Asynchronous health monitoring for MetaMap servers"""
import asyncio
import socket
import logging
import time
from enum import Enum
from typing import Dict, Optional, Callable
from datetime import datetime, timedelta
import subprocess

from ..core.config import PyMMConfig

class ServerStatus(Enum):
    """Server health status"""
    HEALTHY = "healthy"
    DEGRADED = "degraded" 
    DOWN = "down"
    STARTING = "starting"
    UNKNOWN = "unknown"

class HealthMonitor:
    """Lightweight asynchronous health checker with circuit breaker"""
    
    def __init__(self, config: PyMMConfig, server_manager=None):
        self.config = config
        self.server_manager = server_manager
        self.status = {
            "wsd": ServerStatus.UNKNOWN,
            "tagger": ServerStatus.UNKNOWN,
            "metamap": ServerStatus.UNKNOWN
        }
        self.consecutive_failures = {"wsd": 0, "tagger": 0, "metamap": 0}
        self.last_check = {"wsd": None, "tagger": None, "metamap": None}
        self.circuit_breaker_threshold = 3
        self.check_interval = config.get("health_check_interval", 30)
        self.monitoring_task = None
        self.callbacks = []
        self.logger = logging.getLogger("HealthMonitor")
        
    def add_status_callback(self, callback: Callable):
        """Add callback for status changes"""
        self.callbacks.append(callback)
    
    async def check_port_health(self, port: int, service_name: str) -> bool:
        """Quick TCP connect test"""
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection('localhost', port), 
                timeout=2.0
            )
            writer.close()
            await writer.wait_closed()
            return True
        except asyncio.TimeoutError:
            self.logger.debug(f"{service_name} port {port} connection timeout")
            return False
        except Exception as e:
            self.logger.debug(f"{service_name} port {port} connection error: {e}")
            return False
    
    async def check_metamap_integration(self) -> ServerStatus:
        """Lightweight MetaMap connectivity test"""
        if not self.config.get("metamap_binary_path"):
            return ServerStatus.UNKNOWN
            
        try:
            # Create minimal test
            proc = await asyncio.create_subprocess_exec(
                self.config.get("metamap_binary_path"),
                '--XMLf1', '--silent', '-',
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            # Send minimal input
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(b"test"), 
                timeout=5.0
            )
            
            stderr_text = stderr.decode('utf-8', errors='ignore').lower()
            
            # Check for connection errors
            if "spio_e_net_connrefused" in stderr_text:
                self.logger.debug("MetaMap connection refused")
                return ServerStatus.DOWN
            elif "connection" in stderr_text and "error" in stderr_text:
                self.logger.debug("MetaMap connection error detected")
                return ServerStatus.DEGRADED
            elif proc.returncode == 0:
                return ServerStatus.HEALTHY
            else:
                self.logger.debug(f"MetaMap returned code: {proc.returncode}")
                return ServerStatus.DEGRADED
                
        except asyncio.TimeoutError:
            self.logger.debug("MetaMap integration test timeout")
            return ServerStatus.DEGRADED
        except Exception as e:
            self.logger.error(f"MetaMap integration test error: {e}")
            return ServerStatus.DOWN
    
    async def update_service_status(self, service: str, is_healthy: bool):
        """Update service status with circuit breaker logic"""
        old_status = self.status[service]
        
        if is_healthy:
            self.consecutive_failures[service] = 0
            self.status[service] = ServerStatus.HEALTHY
        else:
            self.consecutive_failures[service] += 1
            
            if self.consecutive_failures[service] >= self.circuit_breaker_threshold:
                self.status[service] = ServerStatus.DOWN
                
                # Trigger restart if manager available
                if self.server_manager and service != "metamap":
                    self.logger.warning(f"{service} marked as DOWN, attempting restart...")
                    asyncio.create_task(self.attempt_restart(service))
            else:
                self.status[service] = ServerStatus.DEGRADED
        
        # Notify callbacks if status changed
        if old_status != self.status[service]:
            self.logger.info(f"{service} status changed: {old_status.value} -> {self.status[service].value}")
            for callback in self.callbacks:
                try:
                    callback(service, self.status[service])
                except Exception as e:
                    self.logger.error(f"Callback error: {e}")
    
    async def attempt_restart(self, service: str):
        """Attempt to restart a service"""
        if not self.server_manager:
            return
            
        self.status[service] = ServerStatus.STARTING
        self.logger.info(f"Attempting to restart {service}...")
        
        try:
            # Run restart in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            success = await loop.run_in_executor(
                None, 
                self.server_manager.restart_service, 
                service
            )
            
            if success:
                self.logger.info(f"{service} restart successful")
                self.consecutive_failures[service] = 0
            else:
                self.logger.error(f"{service} restart failed")
                
        except Exception as e:
            self.logger.error(f"Error restarting {service}: {e}")
    
    async def check_all_services(self):
        """Check health of all services"""
        # Check individual ports
        wsd_up = await self.check_port_health(5554, "wsd")
        tagger_up = await self.check_port_health(1795, "tagger")
        
        # Update status
        await self.update_service_status("wsd", wsd_up)
        await self.update_service_status("tagger", tagger_up)
        
        # Check MetaMap integration if base services are up
        if self.status["wsd"] == ServerStatus.HEALTHY and \
           self.status["tagger"] == ServerStatus.HEALTHY:
            mm_status = await self.check_metamap_integration()
            
            is_healthy = mm_status == ServerStatus.HEALTHY
            await self.update_service_status("metamap", is_healthy)
        else:
            self.status["metamap"] = ServerStatus.DEGRADED
        
        # Update check times
        now = datetime.now()
        for service in self.status:
            self.last_check[service] = now
    
    async def monitor_loop(self):
        """Main monitoring loop"""
        self.logger.info("Starting health monitoring...")
        
        while True:
            try:
                await self.check_all_services()
                
                # Log summary if any issues
                if any(s != ServerStatus.HEALTHY for s in self.status.values()):
                    summary = ", ".join(f"{k}:{v.value}" for k,v in self.status.items())
                    self.logger.debug(f"Health status: {summary}")
                    
            except Exception as e:
                self.logger.error(f"Health monitor error: {e}")
            
            await asyncio.sleep(self.check_interval)
    
    def start_monitoring(self):
        """Start the monitoring loop"""
        if self.monitoring_task and not self.monitoring_task.done():
            self.logger.warning("Monitoring already running")
            return
            
        self.monitoring_task = asyncio.create_task(self.monitor_loop())
        self.logger.info("Health monitoring started")
    
    def stop_monitoring(self):
        """Stop the monitoring loop"""
        if self.monitoring_task:
            self.monitoring_task.cancel()
            self.logger.info("Health monitoring stopped")
    
    def get_status_summary(self) -> Dict[str, Dict]:
        """Get detailed status summary"""
        now = datetime.now()
        summary = {}
        
        for service, status in self.status.items():
            summary[service] = {
                "status": status.value,
                "consecutive_failures": self.consecutive_failures[service],
                "last_check": self.last_check[service].isoformat() if self.last_check[service] else None,
                "time_since_check": (now - self.last_check[service]).total_seconds() if self.last_check[service] else None
            }
            
        return summary
    
    def is_healthy(self) -> bool:
        """Check if all services are healthy"""
        return all(s == ServerStatus.HEALTHY for s in self.status.values())
    
    def get_failing_services(self) -> list:
        """Get list of failing services"""
        return [s for s, status in self.status.items() if status != ServerStatus.HEALTHY]