# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import re
from datetime import date, datetime
from typing import Any, Iterable, Mapping, Optional

from elastic_transport.client_utils import DEFAULT, percent_encode

_WARNING_RE = re.compile(r"\"([^\"]*)\"")
# TODO: get versionstr dynamically
_COMPAT_MIMETYPE_TEMPLATE = "application/vnd.elasticsearch+%s; compatible-with=" + str("8.2.0".partition(".")[0])
_COMPAT_MIMETYPE_RE = re.compile(r"application/(json|x-ndjson|vnd\.mapbox-vector-tile)")
_COMPAT_MIMETYPE_SUB = _COMPAT_MIMETYPE_TEMPLATE % (r"\g<1>",)


def _mimetype_header_to_compat(header, request_headers):
    # Converts all parts of a Accept/Content-Type headers
    # from application/X -> application/vnd.elasticsearch+X
    mimetype = request_headers.get(header, None)
    if mimetype:
        request_headers[header] = _COMPAT_MIMETYPE_RE.sub(_COMPAT_MIMETYPE_SUB, mimetype)


def _escape(value: Any) -> str:
    """
    Escape a single value of a URL string or a query parameter. If it is a list
    or tuple, turn it into a comma-separated string first.
    """

    # make sequences into comma-separated stings
    if isinstance(value, (list, tuple)):
        value = ",".join([_escape(item) for item in value])

    # dates and datetimes into isoformat
    elif isinstance(value, (date, datetime)):
        value = value.isoformat()

    # make bools into true/false strings
    elif isinstance(value, bool):
        value = str(value).lower()

    elif isinstance(value, bytes):
        return value.decode("utf-8", "surrogatepass")

    if not isinstance(value, str):
        return str(value)
    return value


def _quote(value: Any) -> str:
    return percent_encode(_escape(value), ",*")


def _quote_query(query: Mapping[str, Any]) -> str:
    return "&".join([f"{k}={_quote(v)}" for k, v in query.items()])
