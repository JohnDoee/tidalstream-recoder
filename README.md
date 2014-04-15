TidalStream ReCoder
=====================

Proxy able to re-encode MKV files to make them playable in some webbrowsers and with Google Chromecast.

This is part of the TidalStream Family and will integrate into the eco-system while
being usable in its independnt state.


Prerequisite
------------

* Python 2.6+
* Twisted Python
* FFMpeg & FFProbe

Installation
------------

    virtualenv tidalstream-recoder
    tidalstream-recoder/bin/pip install git+https://github.com/JohnDoee/tidalstream-recoder.git


Usage
-----

    twistd -n tidalrecoder

Known problems and missing features
-----------------------------------

* If the input file is smaller than the output file, unknown things will happen.
* The first video and audio tag will be used
* No authentication or verification
* Only MKV input and output support
* Not possible to seek beyond currently encoded elements, it should start the encoding from the middle instead.



License
--------
See LICENSE
