"""Containers module."""

from dependency_injector import containers, providers

from nmdc_runtime.domain.users.userService import UserService
from nmdc_runtime.infrastructure.database.impl.mongo.models.user import (
    UserQueries,
)


class Container(containers.DeclarativeContainer):
    user_queries = providers.Singleton(UserQueries)

    user_service = providers.Factory(UserService, user_queries=user_queries)
