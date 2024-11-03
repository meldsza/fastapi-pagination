import base64
import json
from typing import Any, Optional, overload

from ..api import apply_items_transformer, create_page
from ..bases import AbstractParams, is_cursor
from ..types import AdditionalData, ItemsTransformer, SyncItemsTransformer
from ..utils import verify_params
from opensearchpy import Search
from opensearchpy._async.helpers.search import AsyncSearch
from opensearchpy.helpers.response import Response



def _get_cursor_value(cursor: str | bytes) -> Optional[str]:
    try:
        return json.loads(base64.b64decode(cursor))
    except Exception:
        return None


def _generate_cursor_value(cursor_value: Any) -> str:
    return base64.b64encode(json.dumps(cursor_value).encode("utf-8")).decode("utf-8")


def _paginate_cursor(search_client: Search|AsyncSearch,
    params: Optional[AbstractParams] = None,
    *,
    transformer: Optional[ItemsTransformer] = None,
    additional_data: Optional[AdditionalData] = None):
    if search_client._sort is None or len(search_client._sort) ==0:
        search_client = search_client.sort("_id")
    if isinstance(search_client, Search):
        return _paginate_cursor_sync(**locals())
    elif isinstance(search_client, AsyncSearch):
        return _paginate_cursor_async(**locals())
    else:
        raise ValueError("search_client must be an instance of Search or AsyncSearch from opensearchpy")

def _paginate_cursor_sync(
    search_client: Search,
    params: Optional[AbstractParams] = None,
    *,
    transformer: Optional[SyncItemsTransformer] = None,
    additional_data: Optional[AdditionalData] = None,
) -> Any:
    params, raw_params = verify_params(params, "cursor")

    q: Search = search_client.params(track_total_hits=True)
    if raw_params.cursor:
        cursor = _get_cursor_value(raw_params.cursor)
        q = search_client.extra(search_after=cursor)
    results: Response = q.execute()
    total = results.hits.total.value
    items = [x.to_dict() for x in results.hits] 

    t_items = apply_items_transformer(items, transformer)

    next_page = None
    if len(results.hits) >= raw_params.size:
        next_page = _generate_cursor_value(results.hits[-1].meta.to_dict()["sort"])

    return create_page(
        t_items,
        total=total,
        params=params,
        next_=next_page,
        **(additional_data or {}),
    )


async def _paginate_cursor_async(
    search_client: AsyncSearch,
    params: Optional[AbstractParams] = None,
    *,
    transformer: Optional[ItemsTransformer] = None,
    additional_data: Optional[AdditionalData] = None,
) -> Any:
    params, raw_params = verify_params(params, "cursor")

    q: AsyncSearch = search_client.params(track_total_hits=True)
    if raw_params.cursor:
        cursor = _get_cursor_value(raw_params.cursor)
        q = search_client.extra(search_after=cursor)
    results: Response = await q.execute()
    total = results.hits.total.value
    items = [x.to_dict() for x in results.hits] 

    t_items = await apply_items_transformer(items, transformer, async_=True)

    next_page = None
    if len(results.hits) >= raw_params.size:
        next_page = _generate_cursor_value(results.hits[-1].meta.to_dict()["sort"])

    return create_page(
        t_items,
        total=total,
        params=params,
        next_=next_page,
        **(additional_data or {}),
    )

    
def _paginate_offset(
    search_client: Search|AsyncSearch,
    params: Optional[AbstractParams] = None,
    *,
    transformer: Optional[SyncItemsTransformer] = None,
    additional_data: Optional[AdditionalData] = None,
) -> Any:
    if isinstance(search_client, Search):
        return _paginate_offset_sync(**locals())
    elif isinstance(search_client, AsyncSearch):
        return _paginate_offset_async(**locals())
    else:
        raise ValueError("search_client must be an instance of Search or AsyncSearch from opensearchpy")

def _paginate_offset_sync(
    search_client: Search,
    params: Optional[AbstractParams] = None,
    *,
    transformer: Optional[SyncItemsTransformer] = None,
    additional_data: Optional[AdditionalData] = None,
) -> Any:
    params, raw_params = verify_params(params, "limit-offset")

    
    # Since we are using limit offset, we just need to use track_total_hits
    q: Search = search_client.params(track_total_hits=True)
    if raw_params.limit is not None:
        q: Search = search_client.extra(size=raw_params.limit)
    if raw_params.offset is not None:
        q: Search = search_client.extra(from_=raw_params.offset)
    results: Response = q.execute()
    total = results.hits.total.value
    items = [x.to_dict() for x in results.hits] 
    
    t_items = apply_items_transformer(items, transformer)


    return create_page(
        t_items,
        total=total,
        params=params,
        **(additional_data or {}),
    )


async def _paginate_offset_async(
    search_client: AsyncSearch,
    params: Optional[AbstractParams] = None,
    *,
    transformer: Optional[SyncItemsTransformer] = None,
    additional_data: Optional[AdditionalData] = None,
) -> Any:
    params, raw_params = verify_params(params, "limit-offset")

    
    # Since we are using limit offset, we just need to use track_total_hits
    q: AsyncSearch = search_client.params(track_total_hits=True)
    if raw_params.limit is not None:
        q = search_client.extra(size=raw_params.limit)
    if raw_params.offset is not None:
        q = search_client.extra(from_=raw_params.offset)
    results: Response = await q.execute()
    total = results.hits.total.value
    items = [x.to_dict() for x in results.hits] 
    
    t_items = await apply_items_transformer(items, transformer, async_=True)


    return create_page(
        t_items,
        total=total,
        params=params,
        **(additional_data or {}),
    )

@overload
def paginate(
    search_client: Search,
    params: Optional[AbstractParams] = None,
    *,
    transformer: Optional[SyncItemsTransformer] = None,
    additional_data: Optional[AdditionalData] = None,
) -> Any:
    pass

@overload
async def paginate(
    search_client: AsyncSearch,
    params: Optional[AbstractParams] = None,
    *,
    transformer: Optional[ItemsTransformer] = None,
    additional_data: Optional[AdditionalData] = None,
) -> Any:
    pass

def paginate(
    search_client: Any,
    params: Optional[AbstractParams] = None,
    *,
    transformer: Optional[ItemsTransformer] = None,
    additional_data: Optional[AdditionalData] = None,
) -> Any:
    if is_cursor(params):
        return _paginate_cursor(**locals())
    else:
        return _paginate_offset(**locals())