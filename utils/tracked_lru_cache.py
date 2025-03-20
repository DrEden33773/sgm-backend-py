from typing import Any, Protocol

DEFAULT_GROUP_NAME = "global"


class LRUCachedFunction[R](Protocol):
    """Protocol for functions decorated with lru_cache"""

    def __call__(self, *args: Any, **kwargs: Any) -> R: ...
    def cache_clear(self) -> None: ...
    def cache_info(self) -> Any: ...


_TRACKED_CACHE_FUNCTIONS: dict[str, list[LRUCachedFunction[Any]]] = {}


def track_lru_cache_annotated[R](
    fn: LRUCachedFunction[R], group_name: str = DEFAULT_GROUP_NAME
) -> LRUCachedFunction[R]:
    """
    Track an existing lru_cache-decorated function for global cache clearing.

    This decorator must be applied AFTER @lru_cache:

    @track_lru_cache_annotated
    @functools.lru_cache(maxsize=128)
    def my_function(x):
        return x
    """

    # Verify the function has the necessary cache methods
    if not hasattr(fn, "cache_clear") or not hasattr(fn, "cache_info"):
        raise TypeError(
            "The tracked function must be decorated with @lru_cache first. "
            "Apply @track_lru_cache_annotated AFTER @lru_cache."
        )

    # Add to tracked functions
    _TRACKED_CACHE_FUNCTIONS.setdefault(group_name, []).append(fn)

    # Return the original function unchanged
    return fn


def clear_all_tracked_caches(group_name: str = DEFAULT_GROUP_NAME) -> int:
    """
    Clear all cached functions marked with @track_lru_cache_annotated

    Returns:
        int: Number of caches cleared
    """

    if group_name not in _TRACKED_CACHE_FUNCTIONS:
        return 0

    count = len(_TRACKED_CACHE_FUNCTIONS[group_name])
    for fn in _TRACKED_CACHE_FUNCTIONS[group_name]:
        fn.cache_clear()
    return count
