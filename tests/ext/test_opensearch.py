from functools import partial

from fastapi import FastAPI
from pytest import fixture

from fastapi_pagination import LimitOffsetPage, Page, add_pagination
from fastapi_pagination.cursor import CursorPage

from ..base import BasePaginationTestCase

try:
    from opensearchpy import Search
    from opensearchpy._async.helpers.search import AsyncSearch

    from fastapi_pagination.ext.opensearch import paginate
except ImportError:
    Search = None
    AsyncSearch = None
    paginate = None


@fixture(scope="session")
def search_session(opensearch_session):
    return Search(opensearch_session, index="idx")

@fixture(scope="session")
async def async_search_session(async_search_session):
    return AsyncSearch(async_search_session, index="idx")


@fixture(scope="session")
def app():
    return FastAPI()


class TestOpensearchSync(BasePaginationTestCase):

    @fixture(scope="session")
    def app(self, app, search_session, model_cls):
        @app.get("/default", response_model=Page[model_cls])
        @app.get("/limit-offset", response_model=LimitOffsetPage[model_cls])
        @app.get("/cursor", response_model=CursorPage[model_cls])
        def route():
            return paginate(search_session)

        return add_pagination(app)

class TestOpensearchAsync(BasePaginationTestCase):

    @fixture(scope="session")
    def app(self, app, async_search_session, model_cls):
        @app.get("/default", response_model=Page[model_cls])
        @app.get("/limit-offset", response_model=LimitOffsetPage[model_cls])
        @app.get("/cursor", response_model=CursorPage[model_cls])
        async def route():
            return await paginate(async_search_session)

        return add_pagination(app)