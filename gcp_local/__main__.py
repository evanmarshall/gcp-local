"""Allow ``python -m gcp_local`` as an alias of the gcp-local console script."""

import sys

from gcp_local.environment import main

sys.exit(main())
