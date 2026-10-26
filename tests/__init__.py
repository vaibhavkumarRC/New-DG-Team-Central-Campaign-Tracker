import os
# The app no longer ships a built-in admin token (fail-closed when unset), so the
# test suite provides one. Deliberately padded: the app must trim it.
os.environ.setdefault('ADMIN_TOKEN', '  test-admin-token \n')
