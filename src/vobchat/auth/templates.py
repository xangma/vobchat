from dash_bootstrap_components import themes


BOOTSTRAP_CSS = themes.BOOTSTRAP

LOGIN_PAGE = f"""
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>DDME Prototype • Log in</title>
    <link rel="stylesheet" href="{BOOTSTRAP_CSS}">
    <style>
      .form-card {{
        max-width: 540px;
        min-width: 300px;
      }}
    </style>
  </head>
  <body class="bg-light">
    <div class="container py-5">
      <div class="row justify-content-center">
        <div class="form-card bg-white p-4 shadow-sm rounded">
          <h2 class="mb-4">Log in</h2>
          {{% with msgs = get_flashed_messages() %}}
            {{% if msgs %}}
              <div class="alert alert-warning" role="alert">
                {{{{ msgs[0] }}}}
              </div>
            {{% endif %}}
          {{% endwith %}}
          <p class="text-muted small">
            <strong>DDME Prototype:</strong> A Conversational AI Dashboard that
            blends chat, interactive maps, and statistical visualisations so you
            can explore data in a natural, conversational way.
          </p>
          <form class="vstack gap-3" method="post" action="{{{{ base }}}}login">
            <div>
              <label class="form-label">E-mail</label>
              <input class="form-control" name="email" required>
            </div>
            <div>
              <label class="form-label">Password</label>
              <input class="form-control" type="password" name="password" required>
            </div>
            <input type="hidden" name="next" value="{{ request.args.get('next','') }}">
            <button class="btn btn-primary w-100" type="submit">Sign in</button>
          </form>
          <div class="text-center pt-3">
            <a href="{{{{ base }}}}signup">Create account</a>
            &ensp;|&ensp;
            <a href="{{{{ base }}}}signingoogle">Sign in with&nbsp;Google</a>
          </div>
        </div>
      </div>
    </div>
    <footer class="text-center text-muted small pb-3">
      Contact:
      <a href="mailto:xan.morice-atkinson@port.ac.uk">
        xan.morice-atkinson@port.ac.uk
      </a>
    </footer>
  </body>
</html>
"""

LOGIN_PAGE_NO_SIGNUP = LOGIN_PAGE.replace(
    '<a href="{{{{ base }}}}signup">Create account</a> &ensp;|&ensp;', ""
)
