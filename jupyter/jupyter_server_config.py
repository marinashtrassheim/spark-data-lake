# jupyter-server 1.x (used with notebook 6.5.7)
#
# No token/password: convenience for a one-person local demo — this is only
# safe because docker-compose.yaml binds the published port to 127.0.0.1
# (loopback only), not 0.0.0.0. The server itself must still listen on
# 0.0.0.0 *inside* the container for Docker's port publishing to reach it at
# all; the actual network exposure is controlled by the host bind IP, not
# this setting. CSRF protection and CORS stay at their secure defaults.
c = get_config()  # noqa: F821

c.ServerApp.ip = "0.0.0.0"
c.ServerApp.port = 8888
c.ServerApp.open_browser = False
c.ServerApp.root_dir = "/opt/notebooks"
c.ServerApp.token = ""
c.ServerApp.password = ""
