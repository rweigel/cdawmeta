import os

import fastapi
import uvicorn

import cdawmeta

def _init_api(app):
  @app.get("/")
  async def indexhtml(request: fastapi.Request):
    script = os.path.dirname(os.path.realpath(__file__))
    fname = os.path.join(script, 'serve.html')
    logger.info("Reading: " + fname)
    with open(fname) as f:
      indexhtml_ = f.read()
    return fastapi.responses.HTMLResponse(indexhtml_)

  @app.get("/query/")
  async def query(filter: str = "{}"):
    return {"filter": filter}

args = cdawmeta.cli('serve.py')

logger = cdawmeta.logger('serve')
logger.setLevel(args['log_level'].upper())

logger.info("Starting server")
_kwargs = {
            "host": 'localhost',
            "port": 8050,
            "server_header": False
          }

app = fastapi.FastAPI()
_init_api(app)
uvicorn.run(app, **_kwargs)