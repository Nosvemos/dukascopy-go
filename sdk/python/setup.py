# All package metadata lives in pyproject.toml. This file only exists to mark
# the wheel as platform-specific: it bundles a native shared library
# (libdukascopy.*) loaded through ctypes, so it is tied to an OS/architecture
# but not to a Python ABI (tag: py3-none-<platform>).
from setuptools import setup

try:
    from setuptools.command.bdist_wheel import bdist_wheel as _bdist_wheel
except ImportError:  # setuptools < 70.1
    from wheel.bdist_wheel import bdist_wheel as _bdist_wheel


class bdist_wheel(_bdist_wheel):
    def finalize_options(self):
        super().finalize_options()
        self.root_is_pure = False

    def get_tag(self):
        _, _, plat = super().get_tag()
        return "py3", "none", plat


setup(cmdclass={"bdist_wheel": bdist_wheel})
