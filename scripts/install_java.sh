#!/bin/bash
# Quick Java installation script for PythonMetaMap

echo "PythonMetaMap Java Installation Helper"
echo "====================================="

# Detect OS
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # Linux
    if command -v apt-get &> /dev/null; then
        # Debian/Ubuntu
        echo "Detected Debian/Ubuntu system"
        echo "Installing OpenJDK 11..."
        sudo apt-get update
        sudo apt-get install -y openjdk-11-jre-headless
        
    elif command -v yum &> /dev/null; then
        # RHEL/CentOS
        echo "Detected RHEL/CentOS system"
        echo "Installing OpenJDK 11..."
        sudo yum install -y java-11-openjdk
        
    elif command -v dnf &> /dev/null; then
        # Fedora
        echo "Detected Fedora system"
        echo "Installing OpenJDK 11..."
        sudo dnf install -y java-11-openjdk
        
    elif command -v pacman &> /dev/null; then
        # Arch
        echo "Detected Arch Linux"
        echo "Installing OpenJDK 11..."
        sudo pacman -S jre11-openjdk-headless
        
    else
        echo "Unknown Linux distribution"
        echo "Please install Java manually"
        exit 1
    fi
    
elif [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    echo "Detected macOS"
    if command -v brew &> /dev/null; then
        echo "Installing OpenJDK 11 via Homebrew..."
        brew install openjdk@11
        
        # Link for system Java
        sudo ln -sfn $(brew --prefix)/opt/openjdk@11/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-11.jdk
        
        echo "Setting up environment..."
        echo 'export JAVA_HOME=$(/usr/libexec/java_home -v 11)' >> ~/.zshrc
        echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.zshrc
    else
        echo "Homebrew not found. Please install Homebrew first:"
        echo "/bin/bash -c \"\$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)\""
        exit 1
    fi
    
else
    echo "Unsupported OS: $OSTYPE"
    echo "Please install Java manually from: https://adoptium.net/"
    exit 1
fi

# Verify installation
echo ""
echo "Verifying Java installation..."
if java -version 2>&1 | grep -q "openjdk"; then
    echo "Java installed successfully!"
    java -version
    
    # Set JAVA_HOME if not set
    if [ -z "$JAVA_HOME" ]; then
        echo ""
        echo "Setting JAVA_HOME..."
        JAVA_PATH=$(which java)
        JAVA_HOME=$(dirname $(dirname $(readlink -f $JAVA_PATH)))
        export JAVA_HOME
        
        # Add to profile
        if [[ "$OSTYPE" == "linux-gnu"* ]]; then
            echo "export JAVA_HOME=$JAVA_HOME" >> ~/.bashrc
            echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> ~/.bashrc
            echo "Added JAVA_HOME to ~/.bashrc"
        fi
    fi
    
    echo ""
    echo "Java is ready for PythonMetaMap!"
else
    echo "Java installation may have failed. Please check the output above."
    exit 1
fi