# Local Docker demo — no token/password prompt.
c = get_config()  # noqa: F821

c.NotebookApp.ip = "0.0.0.0"
c.NotebookApp.port = 8888
c.NotebookApp.open_browser = False
c.NotebookApp.notebook_dir = "/opt/notebooks"
c.NotebookApp.allow_origin = "*"
c.NotebookApp.disable_check_xsrf = True
c.NotebookApp.token = ""
c.NotebookApp.password = ""
