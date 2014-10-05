Thoth Predictor
===============

Basic configuration
--------------------

Create a file called **application.properties** and add it to the classpath.

Example of the content:

```
thoth.sampling.enabled = true
thoth.sampling.dir = /tmp/thoth/thoth-sampling/
thoth.merging.dir = /tmp/thoth/thoth-merged/
thoth.index.url = http://thoth:8983/solr/
train.dataset.location = /tmp/trained_dataset
test.dataset.location = /tmp/test_dataset
thoth.sampling.schedule = 0 * * * * ?

```
