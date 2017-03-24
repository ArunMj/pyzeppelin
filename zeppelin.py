# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import requests

BASEURI_PATTERN = "http://{host}:{port}/api"

class ZeppelinClient(object):
    """
    Zeppelin rest client for notebook APIs
    """

    def __init__(self, host='localhost', port=6080,auth=None,
                baseuri_pattern=BASEURI_PATTERN,
                **request_extra_opts):
        """
        :param host: hostname
        :param port: port address
        :param auth: tuple containing (username,password) if no authentication is
            enabled, auth should be None
        """
        self.session = requests.Session()
        self.baseuri = baseuri_pattern.format(host=host, port=port)
        if auth:
            self._login(*auth)


    def _login(self,username,password):
        uri = ((self.baseuri + '/login?userName={username}&password={password}')
                .format(username=username, password=password))
        r = self.session.post(uri)
        if not r.ok:
            raise Exception(r.reason + ": cannot login to zeppelin : " + uri)


    def _talk_to_host(self,method,uri,data=None):
        #print uri,
        r = self.session.request(method,uri,data=data)
        # if not r.ok:
        #     raise Exception(r.reason,r.content)
        #print r.content
        return r
    #
    # Notebook REST apis
    # see: https://zeppelin.apache.org/docs/0.7.0/rest-api/rest-notebook.html

    # note operations
    def list_notebooks(self):
        """ Lists all notebooks available on server.
        Returns list of name & id of all notebooks

        Wraps rest call:
            GET http://[zeppelin-server]:[zeppelin-port]/api/notebook
        """
        uri = self.baseuri + '/notebook'
        return self._talk_to_host('GET',uri).json()['body']

    def get_status_of_all_paragraphs(self,noteid):
        """Gets the status of all paragraph by given note id.
        Returns list that compose of the paragraph id,paragraph status,paragraph
        finished date, paragraph started date.

        eg:
           [ { "id":"20151121-212654_766735423","status":"FINISHED",
             "finished":"Tue Nov 24 14:21:40 KST 2015",
             "started":"Tue Nov 24 14:21:39 KST 2015" },...
           ]

        Wraps rest call:
          GET http://[zeppelin-server]:[zeppelin-port]/api/notebook/job/[noteId]
        """
        uri = self.baseuri + '/notebook/job/{noteid}'.format(noteid=noteid)
        return self._talk_to_host('GET',uri).json()['body']

    def get_note_info(self,noteid):
        """ Retrieves an existing note's information using the given id.
        The paragraph  field of the returned dictionary contain information
        about paragraphs in the note.

        Wraps rest call:
            GET	http://[zeppelin-server]:[zeppelin-port]/api/notebook/[noteId]
        """
        uri = self.baseuri + '/notebook/{noteid}'.format(noteid=noteid)
        return self._talk_to_host('GET',uri).json()['body']

    def delete_note(self,noteid):
        """Deletes a note by given note id

        Wraps rest call:
          DELETE http://[zeppelin-server]:[zeppelin-port]/api/notebook/[noteId]
        """
        uri = self.baseuri + '/notebook/{noteid}'.format(noteid=noteid)
        self._talk_to_host('DELETE',uri)


    def clone_note(self,noteid,name=None):
        """ Clones a note by the given id and create a new note using the given
        name or default name of none given.
        Returns id of new notei

        Wraps REST call:
          POST http://[zeppelin-server]:[zeppelin-port]/api/notebook/[noteId]
          {"name": "name of new note"}

        """
        uri = self.baseuri + '/notebook/{noteid}'.format(noteid=noteid)
        data = '{"name":"%s"}' % name if name else None
        r = self._talk_to_host('DELETE',uri,data)
        return r.json()['body']

    def export_note(self,noteid):
        """
        Exports a note by given id

        Wraps REST call:
          GET http://[zeppelin-server]:[zeppelin-port]/api/notebook/export/[noteId]
        """
        raise NotImplementedError

    def import_note(self):
        """ imports note from note json input

        Wraps REST call:
          POST http://[zeppelin-server]:[zeppelin-port]/api/notebook/import
        """
        raise NotImplementedError

    def run_all_paragraph(self, noteid):
        """ Runs all paragraph in the given note id.
        Status 404 : Cannot find note id
               412 : problem with interpreter

        Wraps REST call:
          POST: http://[zeppelin-server]:[zeppelin-port]/api/notebook/job/[noteId]
        """
        uri = self.baseuri + '/notebook/job/{noteid}'.format(noteid=noteid)
        r = self._talk_to_host('POST',uri)

    def stop_all_paragraph(self,noteid):
        """Stops all paragraphs in the given noteid

        Wraps REST call:
          DELETE 	http://[zeppelin-server]:[zeppelin-port]/api/notebook/job/[noteId]

        """
        uri =  self.baseuri + '/notebook/job/{noteid}'.format(noteid=noteid)
        self._talk_to_host('DELETE',uri)

    def clear_all_pargraph_results(self):
        """Clears all paragraph results

        Wraps REST call:
            PUT http://[zeppelin-server]:[zeppelin-port]/api/notebook/[noteId]/clear
        """
        uri =  self.baseuri + '/notebook/job/{noteid}/clear'.format(noteid=noteid)
        self._talk_to_host('PUT',uri)

    # paragraph operations
    def create_paragraph(self, noteid):
        raise NotImplementedError

    def get_paragraph_info(self, noteid,paragraphid):
        raise NotImplementedError

    def get_paragraph_status(self, noteid, paragraphid):
        """Gets the status of a single paragraph by the given note id and
        paragraph id.
        result compose of the paragraph id, paragraph status,
            paragraph finish date, paragraph started date.

        Wraps REST call:
            GET http://[zeppelin-server]:[zeppelin-port]/api/notebook/job/[noteId]/[paragraphId]
        """
        uri = (self.baseuri + '/notebook/job/{noteid}/{paragraphid}'
                    .format(noteid=noteid, paragraphid=paragraphid))
        r = self._talk_to_host('GET',uri)
        return r.json()['body']

    def update_paragraph_config(self, noteid, paragraphid):
        raise NotImplementedError

    def delete_paragraph(self, noteid, paragraphid):
        raise NotImplementedError

    def run_paragraph_asynchronously(self, noteid, paragraphid, payload=None):
        """method runs the paragraph asynchronously by given note and paragraph id.
        This API always return SUCCESS even if the execution of the paragraph
        fails later because the API is asynchronous

        Wraps REST call:
          POST http://[zeppelin-server]:[zeppelin-port]/api/notebook/job/[noteId]/[paragraphId]

        """
        uri = (self.baseuri + '/notebook/job/{noteid}/{paragraphid}'
                    .format(noteid=noteid, paragraphid=paragraphid))
        r = self._talk_to_host('POST', uri, data=payload)
        return r.json()['body']

    def run_paragraph_synchronously(self, noteid, paragraphid, payload=None):
        """Runs the paragraph synchronously by given note and paragraph id.
        This API can return SUCCESS or ERROR depending on the outcome of the paragraph execution
        raises exception if ERROR
        Wraps REST call:
          POST http://[zeppelin-server]:[zeppelin-port]/api/notebook/run/[noteId]/[paragraphId]

        """
        uri = (self.baseuri + '/notebook/run/{noteid}/{paragraphid}'
                    .format(noteid=noteid, paragraphid=paragraphid))
        r = self._talk_to_host('POST', uri, data=payload)
        return r.json()['body']

    def stop_paragraph(self, noteid, paragraphid, payload):
        """Stops the paragraph by given note and paragraph id.

        Wraps REST call:
          DELETE   http://[zeppelin-server]:[zeppelin-port]/api/notebook/job/[noteId]/[paragraphId]
        """
        uri = (self.baseuri + '/notebook/job/{noteid}/{paragraphid}'
                    .format(noteid=noteid, paragraphid=paragraphid))
        r = self._talk_to_host('DELETE', uri)
