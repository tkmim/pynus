"""pynus: """

from pynus.decode import decode_nusdas

def get_version():
    from pathlib import Path
    version_path = Path(__file__).parents[1] / "VERSION"
    with version_path.open() as version_file:
        return version_file.read().strip()

__version__ = get_version()
