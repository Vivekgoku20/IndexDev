from contextlib import asynccontextmanager
from fastapi import FastAPI
from .routers import index_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup

    yield
    # Shutdown
    pass

app = FastAPI(
    title="Stock Index Service",
    description="A service that manages a custom equal-weighted stock index of top 100 US stocks",
    lifespan=lifespan
)

# Include routers
app.include_router(index_router.router, tags=["index"])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
