# Java Installation Guide for PythonMetaMap

MetaMap requires Java to run. This guide will help you install Java on your system.

## Quick Installation

### Ubuntu/Debian
```bash
sudo apt-get update && sudo apt-get install -y openjdk-11-jre-headless
```

### RHEL/CentOS/Fedora
```bash
sudo yum install -y java-11-openjdk
# or for newer versions:
sudo dnf install -y java-11-openjdk
```

### macOS
```bash
brew install openjdk@11
```

### Windows
Download OpenJDK 11 from: https://adoptium.net/

## Automated Installation

You can use our installation script:
```bash
./scripts/install_java.sh
```

This script will:
- Detect your operating system
- Install the appropriate Java package
- Set up JAVA_HOME environment variable
- Verify the installation

## Verifying Installation

After installation, verify Java is working:
```bash
java -version
```

You should see output like:
```
openjdk version "11.0.x" 
OpenJDK Runtime Environment...
```

## Setting JAVA_HOME

If JAVA_HOME is not automatically set, add it to your shell profile:

### Linux (bash)
```bash
echo 'export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

### macOS (zsh)
```bash
echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 11)' >> ~/.zshrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.zshrc
source ~/.zshrc
```

## Troubleshooting

### Java not found after installation
- Restart your terminal
- Check if Java is in PATH: `which java`
- Manually set JAVA_HOME as shown above

### Permission denied errors
- Make sure to use `sudo` for system-wide installation
- Check disk space: Java requires ~200MB

### Wrong Java version
- MetaMap works best with Java 8 or 11
- If you have multiple versions, set JAVA_HOME to point to the correct one

## Next Steps

After installing Java, PythonMetaMap will automatically detect it. Run:
```bash
pymm -i
```

The interactive interface will start, and you can begin processing medical texts with MetaMap.