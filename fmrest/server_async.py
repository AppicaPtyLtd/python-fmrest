import json
import time
from functools import wraps

import requests
from typing import Dict, Any, Optional, Tuple, List

from . import Server
from .exceptions import BadJSON, FileMakerError, FILEMAKER_NOT_FOUND_ERRORS
from .foundset import Foundset
from .record import Record
from .utils import request_async

try:
    import aiohttp
except ImportError as e:
    raise ValueError("Install aiohttp")


class ServerAsync(Server):
    def __repr__(self) -> str:
        return '<ServerAsync logged_in={} database={}>'.format(
            bool(self._token), self.database
        )

    def _with_retry_get_resource(f):
        @wraps(f)
        async def wrapper(self, *args, **kwargs):
            if not self.retry_get_record_on_not_found:
                return f(self, *args, **kwargs)

            attempted = 0
            error = None

            while attempted <= self.retry_get_record_on_not_found_attempts:
                try:
                    result = await f(self, *args, **kwargs)
                    return result
                except FileMakerError as e:
                    error = e

                    if str(e) not in FILEMAKER_NOT_FOUND_ERRORS:
                        raise  # if another error occurred, re-raise the exception

                    time.sleep(self.retry_get_record_on_not_found_delay.total_seconds())
                finally:
                    attempted += 1

                # Attempted N times. Return the latest error
            if error is not None:
                raise error

        return wrapper

    async def create_record_async(self,
                                  layout: str,
                                  field_data: Dict[str, Any],
                                  portals: Optional[Dict[str, Any]] = None,
                                  scripts: Optional[Dict[str, List]] = None) -> Optional[int]:
        payload = self.create_record_prepare_payload(layout, field_data, portals, scripts)
        response = await self._call_filemaker_async(**payload)
        return self.create_record_prepare_result(response)

    @Server._with_auto_relogin
    async def edit_record_async(self,
                                layout: str,
                                record_id: int,
                                field_data: Dict[str, Any],
                                mod_id: Optional[int] = None,
                                portals: Optional[Dict[str, Any]] = None,
                                scripts: Optional[Dict[str, List]] = None) -> bool:
        payload = self.edit_record_prepare_payload(layout,
                                                   record_id,
                                                   field_data,
                                                   mod_id,
                                                   portals,
                                                   scripts)
        await self._call_filemaker_async(**payload)
        return self.edit_record_prepare_result()

    @Server._with_auto_relogin
    async def delete_record_async(self, layout: str, record_id: int, scripts: Optional[Dict[str, List]] = None):
        payload = self.delete_record_prepare_payload(layout,
                                                     record_id,
                                                     scripts)
        await self._call_filemaker_async(**payload)
        return self.delete_record_prepare_response()

    @Server._with_retry_get_resource
    @Server._with_auto_relogin
    async def get_record_async(self,
                               layout: str,
                               record_id: int,
                               portals: Optional[List[Dict]] = None,
                               scripts: Optional[Dict[str, List]] = None) -> Record:
        payload = self.get_record_prepare_payload(layout,
                                                  record_id,
                                                  portals,
                                                  scripts)
        response = await self._call_filemaker_async(**payload)
        return self.get_record_prepare_result(response)

    @Server._with_auto_relogin
    async def perform_script_async(self, layout: str, name: str, param: Optional[str] = None) -> Tuple[Optional[int], Optional[str]]:
        payload = self.perform_script_prepare_payload(layout, name, param)
        response = await self._call_filemaker_async(**payload)
        return self.perform_script_prepare_result(response)

    @Server._with_retry_get_resource
    @Server._with_auto_relogin
    async def get_records_async(self,
                                layout: str,
                                offset: int = 1,
                                limit: int = 100,
                                sort: Optional[List[Dict[str, str]]] = None,
                                portals: Optional[List[Dict[str, Any]]] = None,
                                scripts: Optional[Dict[str, List]] = None) -> Foundset:
        payload = self.get_records_prepare_payload(
                                                   layout,
                                                   offset,
                                                   limit,
                                                   sort,
                                                   portals,
                                                   scripts)
        response = await self._call_filemaker_async(**payload)
        return self.get_records_prepare_result(response, layout)

    @Server._with_retry_get_resource
    @Server._with_auto_relogin
    async def find_async(self,
                         layout: str,
                         query: List[Dict[str, Any]],
                         sort: Optional[List[Dict[str, str]]] = None,
                         offset: int = 1,
                         limit: int = 100,
                         portals: Optional[List[Dict[str, Any]]] = None,
                         scripts: Optional[Dict[str, List]] = None) -> Foundset:
        payload = self.find_prepare_payload(layout,
                                            query,
                                            sort,
                                            offset,
                                            limit,
                                            portals,
                                            scripts)
        response = await self._call_filemaker_async(**payload)
        return self.find_prepare_result(layout, response)

    @Server._with_auto_relogin
    async def fetch_file_async(self,
                               layout: str,
                               file_url: str,
                               stream: bool = False) -> Tuple[str,
                                                        Optional[str],
                                                        Optional[str],
                                                        requests.Response]:
        payload = self.fetch_file_prepare_payload(layout,
                                                  file_url,
                                                  stream)
        response = await self._call_filemaker_async(**payload)
        return self.fetch_file_prepare_result(layout, file_url, response)

    @Server._with_auto_relogin
    async def set_globals_async(self, globals_: Dict[str, Any]) -> bool:
        payload = self.set_globals(globals_)
        await self._call_filemaker_async(**payload)
        return self.set_globals_prepare_result()

    @Server._with_auto_relogin
    async def _call_filemaker_async(self, method: str, path: str,
                                    data: Optional[Dict] = None,
                                    params: Optional[Dict] = None,
                                    **kwargs: Any) -> Dict:
        url = self.url + path
        # if we have a token, make sure it's included in the header
        # if not, the Authorization header gets removed (necessary for example for logout)
        self._update_token_header()
        response_text = await request_async(method,
                                            path=url,
                                            json=data,
                                            params=params,
                                            headers=self._headers,
                                            verify_ssl=self.verify_ssl,
                                            **kwargs)

        try:
            response_data = json.loads(response_text)
        except json.decoder.JSONDecodeError as ex:
            raise BadJSON(ex, response_text) from None

        return self.handle_response_data(response_data)
