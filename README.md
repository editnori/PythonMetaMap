# Pymm: Python Wrapper for MetaMap

Python Wrapper for extracting candidate and mapping concepts using MetaMap. Pymm parses the XML output of the MetaMap. The below concept information are extracted:
*   score
*   matched word
*   cui
*   semtypes
*   negated
*   matched word start position
*   matched word end position
*   ismapping

The flag <code>ismapping</code> is set to True if it is a mapping concept else it is False for a candidate mapping.

## Installation

Clone the repository and install the package in editable mode. Use the
provided helper script to download and set up MetaMap 2020.

```
git clone https://github.com/smujjiga/pymm.git
cd pymm
pip install -e .
python install_metamap.py  # downloads and installs MetaMap
```


## Usage

After MetaMap is installed you can process a directory of text files using the
`pymm-cli` tool:

```
pymm-cli INPUT_DIR OUTPUT_DIR --metamap-path path/to/metamap20
```

The CLI supports parallel processing with the `--workers` option and keeps a
state file in the output directory so interrupted runs can be resumed.

Concept extraction is done via parse method
<pre>
mmos = mm.parse(['heart attack', 'myocardial infarction'])
</pre>

Parse method returns an iterator of Metamap Object iterators corresponding to each input sentence. Each Metamap Object iterator return the candidate and mapping concepts.
<pre>
for idx, mmo in enumerate(mmos):
   for jdx, concept in enumerate(mmo):
     print (concept.cui, concept.score, concept.matched)
     print (concept.semtypes, concept.ismapping)
</pre>
Python MetaMap wrapper object also support debug parameter which persists input and output files as well print the command line used to run the MetaMap

<pre>
mm = Metamap(METAMAP_PATH, debug=True)
</pre>

## Sample
Below shown is a code snippet for extracting concepts on large number of sentences.

<pre>
def read_lines(file_name, fast_forward_to, batch_size, preprocessing):
    sentences = list()
    with open(file_name, 'r') as fp:
        for i in range(fast_forward_to):
            fp.readline()

        for idx, line in enumerate(fp):
            sentences.append(preprocessing(line))
            if (idx+1) % batch_size == 0:
                yield sentences
                sentences.clear()
try:
    for i, sentences in enumerate(read_lines(CLINICAL_TEXT_FILE, last_checkpoint, BATCH_SIZE, clean_text)):
        timeout = 0.33*BATCH_SIZE
        try_again = False
        try:
            mmos = mm.parse(sentences, timeout=timeout)
        except MetamapStuck:
            # Try with larger timeout
            print ("Metamap Stuck !!!; trying with larger timeout")
            try_again = True
        except:
            print ("Exception in mm; skipping the batch")
            traceback.print_exc(file=sys.stdout)
            continue

        if try_again:
            timeout = BATCH_SIZE*2
            try:
                mmos = mm.parse(sentences, timeout=timeout)
            except MetamapStuck:
                # Again stuck; Ignore this batch
                print ("Metamap Stuck again !!!; ignoring the batch")
                continue
            except:
                print ("Exception in mm; skipping the batch")
                traceback.print_exc(file=sys.stdout)
                continue

        for idx, mmo in enumerate(mmos):
            for jdx, concept in enumerate(mmo):
                save(sentences[idx], concept)

        curr_checkpoint = (i+1)*BATCH_SIZE + last_checkpoint
        record_checkpoint(curr_checkpoint)
finally:
    mm.close()
</pre>

## Acknowledgement
This python wrapper is motivated by
https://github.com/AnthonyMRios/pymetamap. Pymetamap parses the MMI output where as Pymm parses XML output. I decided to code Pymm targeting extraction of concept on huge corpus. I have used Pymm to extract candidate and mapping concepts of 10 Million sentence.
