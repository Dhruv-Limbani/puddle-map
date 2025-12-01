import contextlib
from fastapi import FastAPI, Request, HTTPException, status
from dotenv import load_dotenv
import os
from puddle_server.mcp import mcp
# Import tools and prompts so they register with FastMCP on load
import puddle_server.tools.context_tools 
import puddle_server.tools.query_tool 
import puddle_server.tools.inquiry_tools
import puddle_server.prompts
load_dotenv()
API_KEY = os.environ["API_KEY"]

# ASGI middleware for API key authentication
class APIKeyMiddleware:
    def __init__(self, app):
        self.app = app
    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            headers = {k.decode().lower(): v.decode() for k, v in scope["headers"]}
            auth_header = headers.get("authorization")

            # Expecting format: "Bearer <API_KEY>"
            if not auth_header or not auth_header.startswith("Bearer "):
                from starlette.responses import JSONResponse
                response = JSONResponse({"detail": "Missing or invalid Authorization header"}, status_code=401)
                await response(scope, receive, send)
                return

            token = auth_header.split("Bearer ")[-1].strip()
            if token != API_KEY:
                from starlette.responses import JSONResponse
                response = JSONResponse({"detail": "Invalid API Key"}, status_code=401)
                await response(scope, receive, send)
                return

        await self.app(scope, receive, send)

@contextlib.asynccontextmanager
async def lifespan(app: FastAPI):
    async with contextlib.AsyncExitStack() as stack:
        await stack.enter_async_context(mcp.session_manager.run())
        yield

app = FastAPI(lifespan=lifespan)
# app.add_middleware(APIKeyMiddleware)
app.mount("/puddle-mcp", mcp.streamable_http_app())

PORT = os.environ.get("PORT", 8002)