import os
from typing import Dict
import pip
from setuptools import setup, find_packages

NAME = "ecbr-generator"
AUTHOR = "Your Name"
AUTHOR_EMAIL = "your_email@example.com"
DESC = "Library containing ECBR generator logic and configs"
URL = "https://github.com/your-repo-url"

def get_install_requirements():
    working_directory = os.path.abspath(os.path.dirname(__file__))
    requires = []
    try:
        import pipfile
        our_pipfile = pipfile.load(os.path.join(working_directory, "Pipfile"))
        for package, version in our_pipfile.data["default"].items():
            if isinstance(version, Dict):
                requires.append(f"{package}{version['version']}")
            elif isinstance(version, str):
                requires.append(f"{package}{version}")
            else:
                raise ValueError(f"Error parsing dependency: {package}{version}")
    except ImportError:
        pip.main(["install", "pipfile"])
    return requires

setup(
    name=NAME,
    version="1.0.0",
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    description=DESC,
    url=URL,
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    package_data={"ecbr-generator": ["configs/*.yaml"]},
    install_requires=get_install_requirements(),
)


######

import os
from typing import Dict
import pip
from setuptools import setup, find_packages

NAME = "ecbr-consolidations"
AUTHOR = "Your Name"
AUTHOR_EMAIL = "your_email@example.com"
DESC = "Library containing ECBR consolidation logic and configs"
URL = "https://github.com/your-repo-url"

def get_install_requirements():
    working_directory = os.path.abspath(os.path.dirname(__file__))
    requires = []
    try:
        import pipfile
        our_pipfile = pipfile.load(os.path.join(working_directory, "Pipfile"))
        for package, version in our_pipfile.data["default"].items():
            if isinstance(version, Dict):
                requires.append(f"{package}{version['version']}")
            elif isinstance(version, str):
                requires.append(f"{package}{version}")
            else:
                raise ValueError(f"Error parsing dependency: {package}{version}")
    except ImportError:
        pip.main(["install", "pipfile"])
    return requires

setup(
    name=NAME,
    version="1.0.0",
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    description=DESC,
    url=URL,
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    package_data={"ecbr-consolidations": ["configs/*.yaml"]},
    install_requires=get_install_requirements(),
)



#######
import os
from typing import Dict
import pip
from setuptools import setup, find_packages

NAME = "ecbr-calculations"
AUTHOR = "Your Name"
AUTHOR_EMAIL = "your_email@example.com"
DESC = "Library containing ECBR calculation logic and configs"
URL = "https://github.com/your-repo-url"

def get_install_requirements():
    working_directory = os.path.abspath(os.path.dirname(__file__))
    requires = []
    try:
        import pipfile
        our_pipfile = pipfile.load(os.path.join(working_directory, "Pipfile"))
        for package, version in our_pipfile.data["default"].items():
            if isinstance(version, Dict):
                requires.append(f"{package}{version['version']}")
            elif isinstance(version, str):
                requires.append(f"{package}{version}")
            else:
                raise ValueError(f"Error parsing dependency: {package}{version}")
    except ImportError:
        pip.main(["install", "pipfile"])
    return requires

setup(
    name=NAME,
    version="1.0.0",
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    description=DESC,
    url=URL,
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    package_data={"ecbr-calculations": ["configs/*.yaml"]},
    install_requires=get_install_requirements(),
)
