#!/usr/bin/env python


class FSClientException(Exception):
    def __init__(self, *args, **kwargs):
        super(self, Exception).__init__(*args, **kwargs)

