import os
import os.path

from returns.io import impure_safe

exists = impure_safe(os.path.exists)
remove = impure_safe(os.remove)
