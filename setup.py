from setuptools import setup, find_packages

setup(
    name="vobchat",  # Change this to a unique name
    version="0.1.0",
    author="Xan Morice-Atkinson",
    author_email="xan.morice-atkinson@port.ac.uk",
    description="A Dash web application for talking to VoB Data.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    packages=find_packages(),
    install_requires=[
        "dash",
        "dash-bootstrap-components",
        "dash-extensions",
        "pandas",
        "geopandas",
        "shapely",
        "pyproj",
        "plotly",
        "langchain",
        "langgraph",
        "langchain_community",
        "langchain_openai",
        "langchain_ollama",
        "configparser",
        "gunicorn",
        "psycopg2-binary",
        "dash-leaflet",
        "grandalf"
    ],
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "vob=app.main:run",  # Adjust to your main script
        ],
    },
)