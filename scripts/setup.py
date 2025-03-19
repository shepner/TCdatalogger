#!/usr/bin/env python3
"""
Setup script for TCdatalogger development environment.

This script handles:
1. Virtual environment creation and activation
2. Dependency installation
3. Environment configuration
4. Test environment setup
5. Script permissions
6. Directory structure verification
7. Virtual environment activation and command execution
"""

import os
import sys
import venv
import stat
import shutil
import subprocess
import json
from pathlib import Path
import logging
from typing import List, Optional, Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EnvSetup:
    """Handles development environment setup."""
    
    def __init__(self, base_dir: Optional[Path] = None):
        """Initialize setup with base directory."""
        self.base_dir = Path(base_dir) if base_dir else Path(__file__).parent.parent
        self.venv_dir = self.base_dir / '.venv'
        self.requirements_file = self.base_dir / 'app' / 'requirements.txt'
        self.scripts_dir = self.base_dir / 'scripts'
        self.tests_dir = self.base_dir / 'tests'
        self.app_dir = self.base_dir / 'app'
        self.config_dir = self.base_dir / 'config'
        
        # Required directory structure
        self.required_dirs = {
            'scripts': self.scripts_dir,
            'tests': self.tests_dir,
            'tests/unit': self.tests_dir / 'unit',
            'tests/integration': self.tests_dir / 'integration',
            'tests/fixtures': self.tests_dir / 'fixtures',
            'app': self.app_dir,
            'app/core': self.app_dir / 'core',
            'app/services': self.app_dir / 'services',
            'config': self.config_dir
        }

        # Development configuration defaults
        self.dev_config = {
            'log_level': 'DEBUG',
            'enable_metrics': True,
            'metric_prefix': 'custom.googleapis.com/tcdatalogger'
        }

    def ensure_directory_structure(self) -> None:
        """Ensure all required directories exist."""
        logger.info("Verifying directory structure...")
        for name, path in self.required_dirs.items():
            if not path.exists():
                logger.info("Creating directory: %s", name)
                path.mkdir(parents=True, exist_ok=True)
            else:
                logger.info("Directory exists: %s", name)

    def ensure_config_files(self) -> None:
        """Ensure required config files exist with proper structure."""
        logger.info("Checking configuration files...")
        
        # Create example config files if they don't exist
        example_configs = {
            'credentials.json.example': {
                'type': 'service_account',
                'project_id': 'your-project-id',
                'private_key': 'your-private-key',
                'client_email': 'your-service-account@example.com'
            },
            'TC_API_key.json.example': {
                'default': 'your-api-key',
                'faction_40832': 'your-faction-api-key'
            },
            'endpoints.json.example': {
                'members': {
                    'table': 'torn.members',
                    'frequency': 'PT15M',
                    'storage_mode': 'append'
                }
            },
            'dev_config.json': self.dev_config
        }
        
        for filename, content in example_configs.items():
            config_file = self.config_dir / filename
            if not config_file.exists():
                logger.info("Creating example config file: %s", filename)
                with open(config_file, 'w') as f:
                    json.dump(content, f, indent=4)

    def ensure_requirements_file(self) -> None:
        """Ensure requirements.txt exists."""
        logger.info("Checking requirements file...")
        if not self.requirements_file.exists():
            logger.error("requirements.txt not found at %s", self.requirements_file)
            raise FileNotFoundError(f"requirements.txt not found at {self.requirements_file}")

    def make_self_executable(self) -> None:
        """Make this script executable."""
        script_path = Path(__file__)
        current_mode = script_path.stat().st_mode
        executable_mode = current_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH
        
        if current_mode != executable_mode:
            logger.info("Making setup script executable")
            script_path.chmod(executable_mode)

    def get_venv_activate_script(self) -> Path:
        """Get path to virtual environment activation script."""
        if sys.platform == "win32":
            return self.venv_dir / "Scripts" / "activate.bat"
        return self.venv_dir / "bin" / "activate"
        
    def create_venv(self) -> None:
        """Create virtual environment if it doesn't exist."""
        if not self.venv_dir.exists():
            logger.info("Creating virtual environment...")
            venv.create(self.venv_dir, with_pip=True)
            logger.info("Virtual environment created at %s", self.venv_dir)
        else:
            logger.info("Virtual environment already exists at %s", self.venv_dir)

    def get_venv_python(self) -> Path:
        """Get path to virtual environment Python executable."""
        if sys.platform == "win32":
            return self.venv_dir / "Scripts" / "python.exe"
        return self.venv_dir / "bin" / "python"

    def get_venv_pip(self) -> Path:
        """Get path to virtual environment pip executable."""
        if sys.platform == "win32":
            return self.venv_dir / "Scripts" / "pip.exe"
        return self.venv_dir / "bin" / "pip"

    def install_dependencies(self) -> None:
        """Install project dependencies in virtual environment."""
        pip = self.get_venv_pip()
        
        logger.info("Installing dependencies from %s", self.requirements_file)
        try:
            # Upgrade pip first
            subprocess.run(
                [str(pip), "install", "--upgrade", "pip"],
                check=True,
                capture_output=True,
                text=True
            )
            
            # Install requirements
            subprocess.run(
                [str(pip), "install", "-r", str(self.requirements_file)],
                check=True,
                capture_output=True,
                text=True
            )
            logger.info("Dependencies installed successfully")
        except subprocess.CalledProcessError as e:
            logger.error("Failed to install dependencies: %s", e.stderr)
            raise

    def run_in_venv(self, cmd: List[str], **kwargs) -> subprocess.CompletedProcess:
        """Run a command in the virtual environment."""
        env = os.environ.copy()
        
        # Set virtual environment paths
        if sys.platform == "win32":
            env["PATH"] = f"{self.venv_dir / 'Scripts'};{env['PATH']}"
            env["VIRTUAL_ENV"] = str(self.venv_dir)
        else:
            env["PATH"] = f"{self.venv_dir / 'bin'}:{env['PATH']}"
            env["VIRTUAL_ENV"] = str(self.venv_dir)
        
        # Add project root and app directory to PYTHONPATH
        python_path = str(self.base_dir)
        if "PYTHONPATH" in env:
            env["PYTHONPATH"] = f"{python_path}:{env['PYTHONPATH']}"
        else:
            env["PYTHONPATH"] = python_path
        
        # Load development configuration
        dev_config_file = self.config_dir / 'dev_config.json'
        if dev_config_file.exists():
            with open(dev_config_file) as f:
                dev_config = json.load(f)
                env.update({
                    'LOG_LEVEL': dev_config.get('log_level', 'DEBUG'),
                    'ENABLE_METRICS': str(dev_config.get('enable_metrics', True)).lower(),
                    'METRIC_PREFIX': dev_config.get('metric_prefix', 'custom.googleapis.com/tcdatalogger')
                })
        
        # Remove PYTHONHOME if it exists
        env.pop("PYTHONHOME", None)
        
        return subprocess.run(cmd, env=env, **kwargs)

    def cleanup_pycache(self) -> None:
        """Clean up __pycache__ directories."""
        logger.info("Cleaning up __pycache__ directories...")
        try:
            # Find all __pycache__ directories
            pycache_dirs = list(self.base_dir.rglob("__pycache__"))
            
            # Remove each directory
            for cache_dir in pycache_dirs:
                if cache_dir.is_dir():
                    shutil.rmtree(cache_dir)
                    logger.info("Removed: %s", cache_dir)
            
            # Also remove any .pyc files
            pyc_files = list(self.base_dir.rglob("*.pyc"))
            for pyc_file in pyc_files:
                pyc_file.unlink()
                logger.info("Removed: %s", pyc_file)
                
        except Exception as e:
            logger.warning("Error cleaning up __pycache__: %s", str(e))

    def setup(self) -> None:
        """Run complete setup process."""
        try:
            # Clean up __pycache__ directories first
            self.cleanup_pycache()
            
            # Make script executable
            self.make_self_executable()
            
            # Ensure directory structure
            self.ensure_directory_structure()
            
            # Ensure config files exist
            self.ensure_config_files()
            
            # Ensure requirements file
            self.ensure_requirements_file()
            
            # Create and configure virtual environment
            self.create_venv()
            
            # Install dependencies
            self.install_dependencies()
            
            logger.info("Setup completed successfully")
            
        except Exception as e:
            logger.error("Setup failed: %s", str(e))
            raise

    def run_tests(self, test_path: Optional[str] = None) -> None:
        """Run tests using pytest."""
        if test_path is None:
            test_path = str(self.tests_dir)
        
        logger.info("Running tests from %s", test_path)
        try:
            # Clean up __pycache__ before running tests
            self.cleanup_pycache()
            
            # Get the pytest executable from the virtual environment
            pytest_path = self.venv_dir / "bin" / "pytest"
            if sys.platform == "win32":
                pytest_path = self.venv_dir / "Scripts" / "pytest.exe"
            
            if not pytest_path.exists():
                raise RuntimeError("pytest not found in virtual environment. Run setup first.")
            
            # Ensure coverage directory exists
            coverage_dir = self.tests_dir / "coverage"
            coverage_dir.mkdir(parents=True, exist_ok=True)
            
            self.run_in_venv(
                [
                    str(pytest_path), "-v",
                    "--cov=app",
                    f"--cov-report=xml:{coverage_dir}/coverage.xml",
                    f"--cov-report=html:{coverage_dir}/html",
                    test_path
                ],
                check=True
            )
            logger.info("Tests completed successfully")
        except subprocess.CalledProcessError as e:
            logger.error("Tests failed with exit code %d", e.returncode)
            raise
        except RuntimeError as e:
            logger.error(str(e))
            raise

    def activate_venv(self) -> None:
        """Activate the virtual environment in the current process."""
        if not self.venv_dir.exists():
            raise RuntimeError("Virtual environment does not exist. Run setup first.")
        
        venv_path = str(self.venv_dir)
        
        # Update environment variables
        os.environ["VIRTUAL_ENV"] = venv_path
        os.environ["PATH"] = f"{self.venv_dir/'bin'}{os.pathsep}{os.environ['PATH']}"
        
        # Add project root to PYTHONPATH
        python_path = str(self.base_dir)
        if "PYTHONPATH" in os.environ:
            os.environ["PYTHONPATH"] = f"{python_path}:{os.environ['PYTHONPATH']}"
        else:
            os.environ["PYTHONPATH"] = python_path
        
        # Load development configuration
        dev_config_file = self.config_dir / 'dev_config.json'
        if dev_config_file.exists():
            with open(dev_config_file) as f:
                dev_config = json.load(f)
                os.environ.update({
                    'LOG_LEVEL': dev_config.get('log_level', 'DEBUG'),
                    'ENABLE_METRICS': str(dev_config.get('enable_metrics', True)).lower(),
                    'METRIC_PREFIX': dev_config.get('metric_prefix', 'custom.googleapis.com/tcdatalogger')
                })
        
        # Remove PYTHONHOME if it exists
        os.environ.pop("PYTHONHOME", None)
        
        # Update sys.path
        sys.path.insert(0, str(self.base_dir))
        sys.path.insert(0, venv_path)
        
        logger.info("Virtual environment activated: %s", venv_path)

    def run_main(self) -> None:
        """Run the main application."""
        logger.info("Running main application...")
        try:
            # Ensure virtual environment exists
            if not self.venv_dir.exists():
                logger.info("Virtual environment not found, running setup first...")
                self.setup()

            # Get the Python executable from the virtual environment
            python = self.get_venv_python()
            main_script = self.app_dir / "main.py"

            if not main_script.exists():
                raise RuntimeError(f"Main script not found at {main_script}")

            # Run the main script with proper Python path
            self.run_in_venv(
                [str(python), str(main_script)],
                check=True
            )
            logger.info("Main application completed successfully")
        except subprocess.CalledProcessError as e:
            logger.error("Main application failed with exit code %d", e.returncode)
            raise
        except Exception as e:
            logger.error("Failed to run main application: %s", str(e))
            raise

def main():
    """Main setup function."""
    try:
        setup = EnvSetup()
        
        if len(sys.argv) > 1:
            command = sys.argv[1]
            
            if command == "setup":
                # Run complete setup
                setup.setup()
            
            elif command == "test":
                # Ensure virtual environment is set up
                if not setup.venv_dir.exists():
                    setup.setup()
                
                # Run tests with optional path
                test_path = sys.argv[2] if len(sys.argv) > 2 else None
                setup.run_tests(test_path)
            
            elif command == "activate":
                # Activate virtual environment
                setup.activate_venv()
                
            elif command == "run":
                # Run a command in the virtual environment
                if len(sys.argv) < 3:
                    logger.error("No command specified")
                    sys.exit(1)
                
                setup.run_in_venv(sys.argv[2:], check=True)

            elif command == "main":
                # Run the main application
                setup.run_main()
            
            else:
                logger.error("Unknown command: %s", command)
                logger.info("Available commands: setup, test, activate, run, main")
                sys.exit(1)
        else:
            # Default to setup if no command specified
            setup.setup()
            
    except Exception as e:
        logger.error("Operation failed: %s", str(e))
        sys.exit(1)

if __name__ == "__main__":
    main() 