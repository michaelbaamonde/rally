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

import contextvars
import logging
import re
import time
import warnings
from datetime import date, datetime
from typing import Any, Iterable, Mapping, Optional

import certifi
import elastic_transport
import urllib3
from elastic_transport import (
    ApiResponse,
    BinaryApiResponse,
    HeadApiResponse,
    ListApiResponse,
    ObjectApiResponse,
    TextApiResponse,
)
from elastic_transport.client_utils import DEFAULT, percent_encode
from elasticsearch.compat import warn_stacklevel
from elasticsearch.exceptions import (
    HTTP_EXCEPTIONS,
    ApiError,
    ElasticsearchWarning,
    UnsupportedProductError,
)
from urllib3.connection import is_ipaddress

from esrally import doc_link, exceptions
from esrally.async_connection import RallyAsyncElasticsearch
from esrally.client_utils import (
    _COMPAT_MIMETYPE_RE,
    _COMPAT_MIMETYPE_SUB,
    _COMPAT_MIMETYPE_TEMPLATE,
    _WARNING_RE,
    _mimetype_header_to_compat,
    _quote_query,
)
from esrally.sync_connection import RallySyncElasticsearch, _ProductChecker
from esrally.utils import console, convert, versions


class EsClientFactory:
    """
    Abstracts how the Elasticsearch client is created and customizes the client for backwards
    compatibility guarantees that are broader than the library's defaults.
    """

    def __init__(self, hosts, client_options, distribution_version=None):
        # We need to pass a list of connection strings to the client as of elasticsearch-py 8.0
        def host_string(host):
            protocol = "https" if client_options.get("use_ssl") else "http"
            return f"{protocol}://{host['host']}:{host['port']}"

        self.hosts = [host_string(h) for h in hosts]
        self.client_options = dict(client_options)
        self.ssl_context = None
        # This attribute is necessary for the backwards-compatibility logic contained in
        # RallySyncElasticsearch.perform_request() and RallyAsyncElasticsearch.perform_request().
        self.distribution_version = distribution_version
        self.logger = logging.getLogger(__name__)

        masked_client_options = dict(client_options)
        if "basic_auth_password" in masked_client_options:
            masked_client_options["basic_auth_password"] = "*****"
        if "http_auth" in masked_client_options:
            masked_client_options["http_auth"] = (masked_client_options["http_auth"][0], "*****")
        self.logger.info("Creating ES client connected to %s with options [%s]", hosts, masked_client_options)

        # we're using an SSL context now and it is not allowed to have use_ssl present in client options anymore
        if self.client_options.pop("use_ssl", False):
            # pylint: disable=import-outside-toplevel
            import ssl

            self.logger.info("SSL support: on")

            self.ssl_context = ssl.create_default_context(
                ssl.Purpose.SERVER_AUTH, cafile=self.client_options.pop("ca_certs", certifi.where())
            )

            # We call get() here instead of pop() in order to pass verify_certs through as a kwarg
            # to the elasticsearch.Elasticsearch constructor. Setting the ssl_context's verify_mode to
            # ssl.CERT_NONE is insufficient with version 8.0+ of elasticsearch-py.
            if not self.client_options.get("verify_certs", True):
                self.logger.info("SSL certificate verification: off")
                # order matters to avoid ValueError: check_hostname needs a SSL context with either CERT_OPTIONAL or CERT_REQUIRED
                self.ssl_context.check_hostname = False
                self.ssl_context.verify_mode = ssl.CERT_NONE

                self.logger.warning(
                    "User has enabled SSL but disabled certificate verification. This is dangerous but may be ok for a "
                    "benchmark. Disabling urllib warnings now to avoid a logging storm. "
                    "See https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings for details."
                )
                # disable:  "InsecureRequestWarning: Unverified HTTPS request is being made. Adding certificate verification is strongly \
                # advised. See: https://urllib3.readthedocs.io/en/latest/advanced-usage.html#ssl-warnings"
                urllib3.disable_warnings()
            else:
                # check_hostname should not be set when host is an IP address
                self.ssl_context.check_hostname = self._only_hostnames(hosts)
                self.ssl_context.verify_mode = ssl.CERT_REQUIRED
                self.logger.info("SSL certificate verification: on")

            # When using SSL_context, all SSL related kwargs in client options get ignored
            client_cert = self.client_options.pop("client_cert", False)
            client_key = self.client_options.pop("client_key", False)

            if not client_cert and not client_key:
                self.logger.info("SSL client authentication: off")
            elif bool(client_cert) != bool(client_key):
                self.logger.error("Supplied client-options contain only one of client_cert/client_key. ")
                defined_client_ssl_option = "client_key" if client_key else "client_cert"
                missing_client_ssl_option = "client_cert" if client_key else "client_key"
                console.println(
                    "'{}' is missing from client-options but '{}' has been specified.\n"
                    "If your Elasticsearch setup requires client certificate verification both need to be supplied.\n"
                    "Read the documentation at {}\n".format(
                        missing_client_ssl_option,
                        defined_client_ssl_option,
                        console.format.link(doc_link("command_line_reference.html#client-options")),
                    )
                )
                raise exceptions.SystemSetupError(
                    "Cannot specify '{}' without also specifying '{}' in client-options.".format(
                        defined_client_ssl_option, missing_client_ssl_option
                    )
                )
            elif client_cert and client_key:
                self.logger.info("SSL client authentication: on")
                self.ssl_context.load_cert_chain(certfile=client_cert, keyfile=client_key)
        else:
            self.logger.info("SSL support: off")

        if self._is_set(self.client_options, "basic_auth_user") and self._is_set(self.client_options, "basic_auth_password"):
            self.logger.info("HTTP basic authentication: on")
            self.client_options["basic_auth"] = (self.client_options.pop("basic_auth_user"), self.client_options.pop("basic_auth_password"))
        else:
            self.logger.info("HTTP basic authentication: off")

        if self._is_set(self.client_options, "compressed"):
            console.warn("You set the deprecated client option 'compressed‘. Please use 'http_compress' instead.", logger=self.logger)
            self.client_options["http_compress"] = self.client_options.pop("compressed")

        if self._is_set(self.client_options, "http_compress"):
            self.logger.info("HTTP compression: on")
        else:
            self.logger.info("HTTP compression: off")

        if self._is_set(self.client_options, "enable_cleanup_closed"):
            self.client_options["enable_cleanup_closed"] = convert.to_bool(self.client_options.pop("enable_cleanup_closed"))

    @staticmethod
    def _only_hostnames(hosts):
        has_ip = False
        has_hostname = False
        for host in hosts:
            is_ip = is_ipaddress(host["host"])
            if is_ip:
                has_ip = True
            else:
                has_hostname = True

        if has_ip and has_hostname:
            raise exceptions.SystemSetupError("Cannot verify certs with mixed IP addresses and hostnames")

        return has_hostname

    def _is_set(self, client_opts, k):
        try:
            return client_opts[k]
        except KeyError:
            return False

    def create(self):
        distro = self.distribution_version

        class VersionedSyncClient(RallySyncElasticsearch):
            def __init__(self, *args, **kwargs):
                if distro is not None:
                    self.distribution_version = versions.Version.from_string(distro)
                else:
                    self.distribution_version = None
                super().__init__(*args, **kwargs)

        return VersionedSyncClient(hosts=self.hosts, ssl_context=self.ssl_context, **self.client_options)

    def create_async(self):
        # pylint: disable=import-outside-toplevel
        import io

        import aiohttp
        import elasticsearch
        from elasticsearch.serializer import JSONSerializer

        import esrally.async_connection

        class LazyJSONSerializer(JSONSerializer):
            def loads(self, data):
                meta = RallyAsyncElasticsearch.request_context.get()
                if "raw_response" in meta:
                    return io.BytesIO(data)
                else:
                    return super().loads(data)

        async def on_request_start(session, trace_config_ctx, params):
            RallyAsyncElasticsearch.on_request_start()

        async def on_request_end(session, trace_config_ctx, params):
            RallyAsyncElasticsearch.on_request_end()

        trace_config = aiohttp.TraceConfig()
        trace_config.on_request_start.append(on_request_start)
        trace_config.on_request_end.append(on_request_end)
        # ensure that we also stop the timer when a request "ends" with an exception (e.g. a timeout)
        trace_config.on_request_exception.append(on_request_end)

        # override the builtin JSON serializer
        self.client_options["serializer"] = LazyJSONSerializer()
        self.client_options["trace_config"] = trace_config

        client_options = self.client_options
        distro = self.distribution_version

        class RallyAsyncTransport(elastic_transport.AsyncTransport):
            def __init__(self, *args, **kwargs):
                # We need to pass a trace config to the session that's created in
                # async_connection.RallyAiohttphttpnode, which is a subclass of
                # elastic_transport.AiohttpHttpNode.
                #
                # Its constructor only accepts an elastic_transport.NodeConfig object.
                # Because we do not fully control creation of these objects , we need to
                # pass the trace_config by adding it to the NodeConfig's `extras`, which
                # can contain arbitrary metadata.
                client_options.update({"trace_config": [trace_config]})
                node_configs = args[0]
                for conf in node_configs:
                    extras = conf._extras
                    extras.update({"_rally_client_options": client_options})
                    conf._extras = extras
                original_args = args
                new_args = (node_configs, *original_args[1:])

                super().__init__(*new_args, node_class=esrally.async_connection.RallyAiohttpHttpNode, **kwargs)

        class VersionedAsyncClient(RallyAsyncElasticsearch):
            def __init__(self, *args, **kwargs):
                if distro is not None:
                    self.distribution_version = versions.Version.from_string(distro)
                else:
                    self.distribution_version = None
                super().__init__(*args, **kwargs)

        # max_connections and trace_config are not valid kwargs, so we pop them
        max = self.client_options.pop("max_connections")
        self.client_options.pop("trace_config")

        return VersionedAsyncClient(
            hosts=self.hosts,
            transport_class=RallyAsyncTransport,
            ssl_context=self.ssl_context,
            maxsize=max,
            **self.client_options,
        )


def wait_for_rest_layer(es, max_attempts=40):
    """
    Waits for ``max_attempts`` until Elasticsearch's REST API is available.

    :param es: Elasticsearch client to use for connecting.
    :param max_attempts: The maximum number of attempts to check whether the REST API is available.
    :return: True iff Elasticsearch's REST API is available.
    """
    # assume that at least the hosts that we expect to contact should be available. Note that this is not 100%
    # bullet-proof as a cluster could have e.g. dedicated masters which are not contained in our list of target hosts
    # but this is still better than just checking for any random node's REST API being reachable.
    expected_node_count = len(es.transport.node_pool)
    logger = logging.getLogger(__name__)
    for attempt in range(max_attempts):
        logger.debug("REST API is available after %s attempts", attempt)
        # pylint: disable=import-outside-toplevel
        import elasticsearch

        try:
            # see also WaitForHttpResource in Elasticsearch tests. Contrary to the ES tests we consider the API also
            # available when the cluster status is RED (as long as all required nodes are present)
            es.cluster.health(wait_for_nodes=">={}".format(expected_node_count))
            logger.info("REST API is available for >= [%s] nodes after [%s] attempts.", expected_node_count, attempt)
            return True
        except elasticsearch.ConnectionError as e:
            if "SSL: UNKNOWN_PROTOCOL" in str(e):
                raise exceptions.SystemSetupError("Could not connect to cluster via https. Is this an https endpoint?", e)
            else:
                logger.debug("Got connection error on attempt [%s]. Sleeping...", attempt)
                time.sleep(3)
        # TODO: distinguish between TransportError and ApiError
        except elasticsearch.TransportError as e:
            # cluster block, x-pack not initialized yet, our wait condition is not reached
            if e.message in (503, 401, 408):
                logger.debug("Got status code [%s] on attempt [%s]. Sleeping...", e.message, attempt)
                time.sleep(3)
            else:
                logger.warning("Got unexpected status code [%s] on attempt [%s].", e.message, attempt)
                raise e
    return False
