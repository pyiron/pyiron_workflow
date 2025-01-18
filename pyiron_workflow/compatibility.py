from sys import version_info

if version_info.minor < 11:
    from typing_extensions import Self as Self
else:
    from typing import Self as Self
