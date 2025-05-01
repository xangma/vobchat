# src/vobchat/assets/loginpage.py

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
      /* extra micro-tweaks */
      .form-card {{
        max-width: 540px;      /* hard cap */
        min-width: 300px;      /* never smaller than this */
      }}
    </style>
  </head>

  <body class="bg-light">

    <div class="container py-5">
      <div class="row justify-content-center">
        <div class="form-card bg-white p-4 shadow-sm rounded">

          <!-- heading -->
          <h2 class="mb-4">Log in</h2>

          {{% with msgs = get_flashed_messages() %}}
            {{% if msgs %}}
              <div class="alert alert-warning" role="alert">
                {{{{ msgs[0] }}}}
              </div>
            {{% endif %}}
          {{% endwith %}}

          <!-- project blurb -->
          <p class="text-muted small">
            <strong>DDME Prototype:</strong> A Conversational AI Dashboard that
            blends chat, interactive maps, and statistical visualisations so you
            can explore data in a natural, conversational way.
          </p>

          <!-- login form -->
          <form class="vstack gap-3" method="post" action="/login">
            <div>
              <label class="form-label">E-mail</label>
              <input class="form-control" name="email" required>
            </div>
            <div>
              <label class="form-label">Password</label>
              <input class="form-control" type="password" name="password" required>
            </div>
            <button class="btn btn-primary w-100" type="submit">Sign in</button>
          </form>

          <!-- links -->
          <div class="text-center pt-3">
            <a href="/signup">Create account</a>
            &ensp;|&ensp;
            <a href="/signingoogle">Sign in with&nbsp;Google</a>
          </div>

        </div>
      </div>
    </div>

    <!-- footer -->
    <footer class="text-center text-muted small pb-3">
      Contact: <a href="mailto:xan.morice-atkinson@port.ac.uk">
                xan.morice-atkinson@port.ac.uk</a>
    </footer>

  </body>
</html>
"""

SIGNUP_FORM_HTML = LOGIN_PAGE.replace(
    "Log in", "Sign up"
).replace(
    'action="/login"', 'action="/signup"'
).replace(
    '<button class="btn btn-primary w-100" type="submit">Sign in</button>',
    '<button class="btn btn-success w-100" type="submit">Create account</button>'
).replace(
    '<a href="/signup">Create account</a>', ''
)