# Local Docker demo — no token/password prompt. Safe only because the host
# port is bound to 127.0.0.1 in docker-compose.yaml (loopback only); the
# server still listens on 0.0.0.0 inside the container so Docker's port
# publishing can reach it. CSRF protection and CORS stay at secure defaults.
c = get_config()  # noqa: F821

c.NotebookApp.ip = "0.0.0.0"
c.NotebookApp.port = 8888
c.NotebookApp.open_browser = False
c.NotebookApp.notebook_dir = "/opt/notebooks"
c.NotebookApp.token = ""
c.NotebookApp.password = ""
