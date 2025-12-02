#!/usr/bin/env python3
"""
Generate API documentation using pdoc3

This script generates HTML documentation for all Python modules in the src/ directory.
The documentation is output to the docs/ directory.

Usage:
    python generate_docs.py
"""

import subprocess
import sys
import os

def main():
    """Generate API documentation."""
    print("Generating API documentation...")
    
    # Ensure pdoc3 is installed
    print("Checking for pdoc3...")
    try:
        import pdoc
        print("pdoc3 is installed.")
    except ImportError:
        print("pdoc3 not found. Installing...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pdoc3"])
        print("pdoc3 installed successfully.")
    
    # Generate documentation
    docs_dir = os.path.join(os.path.dirname(__file__), "docs")
    print(f"Generating HTML documentation to {docs_dir}...")
    
    cmd = [
        sys.executable, "-m", "pdoc",
        "--html",
        "--output-dir", "docs",
        "--force",
        "src"
    ]
    
    try:
        subprocess.check_call(cmd)
        print("\n✓ Documentation generated successfully!")
        print(f"✓ Open docs/src/index.html in your browser to view the API documentation.")
    except subprocess.CalledProcessError as e:
        print(f"\n✗ Error generating documentation: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
