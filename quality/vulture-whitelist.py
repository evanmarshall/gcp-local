"""Names that are used only through a framework contract, not through ownership.

Vulture reads this file as ordinary source, so naming an identifier here marks it as
used. Every entry states the contract that requires the name.
"""

# urllib.request.HTTPRedirectHandler.redirect_request override signature.
fp  # noqa: F821
msg  # noqa: F821
newurl  # noqa: F821
req  # noqa: F821

# signal.signal handler signature: (signum, frame).
frame  # noqa: F821
signum  # noqa: F821

# pytest fixture arguments are consumed for their setup side effect, not referenced.
controlled_transport  # noqa: F821
