CDNSim
=========

What is CDNSim
------------------

CDNSim is a stream-level simulator written in Python, designed to simulate a large content delivery network.

## What you need to run CDNSim

### Input files that must be provided:
------------------

   **-links**    - [IRL AS-to-AS links, 'monthly' format](http://irl.cs.ucla.edu/topology/)  
   **-origin**   - [IRL IPv4 origin prefixes, 'monthly' format](http://irl.cs.ucla.edu/topology/)  
   **-rank**     - [CAIDA AS rank data](http://as-rank.caida.org/)  
   **-geo**      - county codes (e.g., 'de' or 'fr,de'). Files should be stored in 'geoAS' directory (examples provided). Geographical data can be obtained from [here](http://www.tcpiputils.com/browse/as).

------------------

All this data are available online, therefore the examples are only given to show how the data are expected to be organized. Each example file includes only the first one hundred entries.

 
## CDNSim uses Python2 and some libraries

### py2-ipaddress:

*Python 2.7 backport* of 3.3's ipaddress module.

pip install py2-ipaddress

or visit: https://pypi.python.org/pypi/py2-ipaddress/

### SubnetTree:

pip install pysubnettree

or on Ubuntu/Debian: sudo apt-get install python-subnettree

or visit: https://pypi.python.org/pypi/pysubnettree/

### NetworkX:

pip install networkx

or on Ubuntu/Debian: sudo apt-get install python-networkx

or visit: https://networkx.github.io/

### matplotlib:

pip install matplotlib

or on Ubuntu/Debian: sudo apt-get install python-matplotlib

or visit: http://matplotlib.org/

### treap:

pip install treap

or visit https://pypi.python.org/pypi/treap


## CDN-Sim Parameters

    usage: cdnsim.py [-h] [-trace file] [-links file] [-origin file] [-rank file]
                     [-geo string] [-nhosts number] [-active number]
                     [-backnoise number] [-streaming] [-ondemandCache]
                     [-percentCache number] [-hierarchical] [-cachesec number]
                     [-cacheinit number] [-cachethreshold number] [-interactive]
                     [-reqRate number] [-scenario file] [-endtime number]
                     [-waitCacheBoot] [-unlimCoreLinkBandwidth] [-siminfo text]
                     [-figures] [-allfigures] [-parallel]
    
    CDN-Sim in Python
    
    optional arguments:
      -h, --help               show this help message and exit
    
    Input files:
      -trace file              User behavior trace (default: usr_trace.dat)
      -links file              IRL AS-to-AS links (default: as_links.dat)
      -origin file             IRL origin prefixes (default: origin.dat)
      -rank file               CAIDA AS rank data (default: caida.org.dat)
    
    Simulation setup:
      -geo string              Comma-separated list of countries (default: de)
      -nhosts number           Maximal number of hosts (default: 1000000)
      -active number           Simultaneously active streams (default: 100000)
      -backnoise number        Simultaneous active background streams (default: 0)
      -streaming               Live streaming (not VoD) (default: True)
      -ondemandCache           Create caches on demand (default: False)
      -percentCache number     % of ASes with static cache (default: 0)
      -hierarchical            Use hierarchical cache placement (default: False)
      -cachesec number         # seconds of video to keep in cache (default: 10)
      -cacheinit number        ondemand cache init time (default: 0.1)
      -cachethreshold number   # streams to start a cache (default: 1)
      -interactive             Interactively populate ASes (default: False)
      -reqRate number          Request rate per min (0-auto) (default: 0)
      -scenario file           Scenario file (format: time, rate/min) (default: )
      -endtime number          Finalize simulation, no new requests (default: 30)
      -waitCacheBoot           Wait cache to boot or bypass it (default: True)
      -unlimCoreLinkBandwidth  Set no limit to the core link bandwidth (default:
                               False)
    
    Results:
      -siminfo text            Name of the simulation (default: )
      -figures                 Figures with results (default: False)
      -allfigures              Figures for all user streams (default: False)
      -parallel                Enable parallelism in simulation (default: False)

