#!/bin/bash
# .platform/hooks/prebuild/01_install_build_deps.sh

echo "Running prebuild script: Installing build dependencies..."

# Update package lists (good practice)
dnf update -y

# Install common build tools (compilers, make, etc.)
# Using groupinstall ensures common tools are present
dnf groupinstall -y "Development Tools"

# Install Python development headers (CRUCIAL for compiling C extensions)
# dnf should automatically pick the right version for the platform's Python
dnf install -y python3-devel

# Install PostgreSQL development headers (needed for psycopg2-binary compilation)
dnf install -y postgresql-devel

# Install common scientific library headers (often needed by NumPy/SciPy/Pandas)
dnf install -y lapack-devel blas-devel

echo "Finished installing build dependencies."

# Exit with 0 to indicate success
exit 0