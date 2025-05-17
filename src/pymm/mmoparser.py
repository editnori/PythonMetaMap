#!/usr/bin/env python
# -*- coding: utf-8 -*-

import collections
from xml.dom.minidom import parse as parse_xml

# Mapping of concept attributes to XML tag names
candidate_mapping = {
    "score": "CandidateScore",
    "cui": "CandidateCUI",
    "semtypes": "SemType",
    "sources": "Source",
    "ismapping": None,
    "matched": "CandidateMatched",
    "isnegated": "Negated",
    "matchedstart": None,
    "matchedend": None,
    "pos_start": None,
    "pos_length": None,
    "phrase_start": None,
    "phrase_length": None,
    "phrase_text": None,
    "utterance_id": None,
}


class Concept(collections.namedtuple("Concept", list(candidate_mapping.keys()))):
    """A lightweight immutable container for a MetaMap *concept*.

    Coordinate conventions
    ---------------------
    MetaMap's native XML reports character offsets in **0-based start, length**
    (via the <Position x="" y=""/> tag).  Older releases encode one or more
    *start/len* tokens in `<PositionalInfo>` where *start* is again 0-based.

    For historical reasons this wrapper stores **1-based** `pos_start` so that
    the value can be copied straight into spreadsheets that are shared with
    the Java API users (the Java demo prints `x` unchanged which is 1-based in
    their CSV).  To simplify maths two helper properties expose the same span
    in 0-based form without allocating a new object:

    * ``pos_start0`` – 0-based start (or ``None``)
    * ``pos_end0``   – 0-based *exclusive* end (or ``None``)
    """
    @classmethod
    def from_xml(cls, candidate, is_mapping=False):
        def get_data(candidate, tag_name):
            """Return text of the first *tag_name* child element or "" if absent.

            Traverses until the first TEXT_NODE to avoid returning empty strings when
            the tag contains wrapper elements (<i>, <b>, etc.)."""
            nodes = candidate.getElementsByTagName(tag_name)
            if not nodes:
                return ""
            node = nodes[0]
            child = node.firstChild
            while child is not None and child.nodeType != node.TEXT_NODE:
                child = child.nextSibling
            if child and child.nodeType == node.TEXT_NODE:
                text = child.data.strip() if child.data else ""
                return text
            return ""

        # Extract Source vocabulary strings (recursively found under <Source> within <Sources>)
        sources = []
        # Case 1: individual <Source> elements
        for src_node in candidate.getElementsByTagName("Source"):
            if src_node.childNodes and src_node.childNodes[0].nodeType == src_node.TEXT_NODE:
                sources.append(src_node.childNodes[0].data.strip())
        # Case 2: single <Sources> element containing a pipe-separated list
        for srcs_node in candidate.getElementsByTagName("Sources"):
            if srcs_node.childNodes and srcs_node.childNodes[0].nodeType == srcs_node.TEXT_NODE:
                raw = srcs_node.childNodes[0].data.strip()
                # Split on common separators
                for token in raw.replace("|", ":").replace(",", ":").split(":"):
                    token = token.strip()
                    if token:
                        sources.append(token)

        # De-duplicate while preserving order
        sources = list(dict.fromkeys(sources))

        # Extract first PositionalInfo (e.g., "123/8") to character start & length
        pos_start_val = None
        pos_len_val = None
        pos_nodes = candidate.getElementsByTagName("PositionalInfo")
        if pos_nodes:
            raw_pos_tokens = []
            for pn in pos_nodes:
                if pn.childNodes and pn.childNodes[0].data.strip():
                    raw_pos_tokens.append(pn.childNodes[0].data.strip())
                else:
                    attr_start = pn.getAttribute("start") or pn.getAttribute("Start") or ""
                    attr_len = pn.getAttribute("length") or pn.getAttribute("Length") or ""
                    if attr_start and attr_len:
                        raw_pos_tokens.append(f"{attr_start}/{attr_len}")
            raw_pos_text = " ".join(raw_pos_tokens).strip()
        else:
            raw_pos_text = ""

        if raw_pos_text:
            # Positional tokens may be separated by space or semicolon. Each token is start/len
            all_tokens = []
            for token_chunk in raw_pos_text.replace(";", " ").split():
                token_chunk = token_chunk.strip()
                if token_chunk and "/" in token_chunk:
                    all_tokens.append(token_chunk)

            if all_tokens:
                try:
                    starts = []
                    ends = []
                    for tok in all_tokens:
                        s_str, l_str = tok.split("/", 1)
                        s = int(s_str)
                        l = int(l_str)
                        starts.append(s)
                        ends.append(s + l)
                    # Calculate length based on 0-based start before converting start to 1-based
                    min_start_0_based = min(starts)
                    pos_len_val = max(ends) - min_start_0_based
                    pos_start_val = min_start_0_based + 1 # Make start 1-based to match Java
                except Exception:
                    pos_start_val = None
                    pos_len_val = None
        
        # Try alternate position source: direct Position tag (common in MetaMap 2020 output)
        if pos_start_val is None:
            position_nodes = candidate.getElementsByTagName("Position")
            if position_nodes and position_nodes[0].hasAttribute("x") and position_nodes[0].hasAttribute("y"):
                try:
                    # Convert to 1-based start for consistency
                    pos_start_val = int(position_nodes[0].getAttribute("x")) + 1
                    pos_len_val = int(position_nodes[0].getAttribute("y"))
                except (ValueError, IndexError):
                    pass

        # ---- Phrase-level positional info (span of whole phrase) ----
        phrase_start_val = None
        phrase_len_val = None
        node_ptr = candidate
        while node_ptr is not None and node_ptr.nodeType == node_ptr.ELEMENT_NODE:
            if node_ptr.tagName == "Phrase":
                phrase_node = node_ptr
                break
            node_ptr = node_ptr.parentNode
        else:
            phrase_node = None

        if phrase_node:
            p_pos_nodes = phrase_node.getElementsByTagName("PositionalInfo")
            raw_phrase_pos = ""  # Initialize raw_phrase_pos
            if p_pos_nodes:
                if p_pos_nodes[0].childNodes:
                    raw_phrase_pos = p_pos_nodes[0].childNodes[0].data.strip()
                else:
                    # Some MetaMap builds encode positional info as attributes on the PositionalInfo tag
                    attr_start_p = p_pos_nodes[0].getAttribute("start") or p_pos_nodes[0].getAttribute("Start") or ""
                    attr_len_p = p_pos_nodes[0].getAttribute("length") or p_pos_nodes[0].getAttribute("Length") or ""
                    if attr_start_p and attr_len_p:
                        raw_phrase_pos = f"{attr_start_p}/{attr_len_p}"
            
            # If child <PositionalInfo> did not yield data, try 'Pos' attribute on <Phrase> tag
            if not raw_phrase_pos:
                phrase_pos_attr = phrase_node.getAttribute("Pos")
                if phrase_pos_attr:
                    raw_phrase_pos = phrase_pos_attr.strip()

            if raw_phrase_pos:
                tokens = []
                for tok in raw_phrase_pos.replace(";", " ").split():
                    tok = tok.strip()
                    if tok and "/" in tok:
                        tokens.append(tok)
                if tokens:
                    try:
                        starts = []
                        ends = []
                        for tok in tokens:
                            s_str, l_str = tok.split("/", 1)
                            s = int(s_str)
                            l = int(l_str)
                            starts.append(s)
                            ends.append(s + l)
                        # Calculate length based on 0-based start before converting start to 1-based
                        min_start_0_based_phrase = min(starts)
                        phrase_len_val = max(ends) - min_start_0_based_phrase
                        phrase_start_val = min_start_0_based_phrase + 1 # Make start 1-based
                    except Exception:
                        phrase_start_val = None
                        phrase_len_val = None

        # If concept-specific coordinates were not found, but phrase-specific ones were,
        # fall back to using phrase coordinates for the concept as well.
        if pos_start_val is None and phrase_start_val is not None:
            pos_start_val = phrase_start_val
            pos_len_val = phrase_len_val

        # ---- Extract full phrase text ----
        phrase_text_val = None
        if phrase_node:
            # Priority 1: <PhraseText> child tag
            phrase_text_nodes = phrase_node.getElementsByTagName("PhraseText")
            if phrase_text_nodes and phrase_text_nodes[0].childNodes:
                phrase_text_val = phrase_text_nodes[0].childNodes[0].data.strip()
            # Priority 2: text attribute on <Phrase>
            if not phrase_text_val and phrase_node.hasAttribute("text"):
                phrase_text_val = phrase_node.getAttribute("text").strip()
            # Priority 3: direct text child (rare)
            if not phrase_text_val and phrase_node.childNodes:
                first_child = phrase_node.childNodes[0]
                if first_child.nodeType == first_child.TEXT_NODE:
                    phrase_text_val = first_child.data.strip()

        # Fallback to CandidateMatched if phrase text still missing
        if not phrase_text_val:
            try:
                phrase_text_val = get_data(candidate, "CandidateMatched")
            except Exception:
                phrase_text_val = ""

        # ---- Extract utterance ID ----
        utterance_id_val = None
        # Climb the DOM tree looking for Utterance ancestor
        node_ptr = candidate
        while node_ptr is not None and node_ptr.nodeType == node_ptr.ELEMENT_NODE:
            if node_ptr.tagName == "Utterance":
                # Try standard id attribute
                if node_ptr.hasAttribute("id"):
                    try:
                        utterance_id_val = int(node_ptr.getAttribute("id"))
                    except ValueError:
                        utterance_id_val = None
                # Try alternate formats if no standard ID
                elif node_ptr.hasAttribute("Index") or node_ptr.hasAttribute("index"):
                    try:
                        utterance_id_val = int(node_ptr.getAttribute("Index") or node_ptr.getAttribute("index"))
                    except ValueError:
                        utterance_id_val = None
                elif node_ptr.hasAttribute("number") or node_ptr.hasAttribute("Number"):
                    try:
                        utterance_id_val = int(node_ptr.getAttribute("number") or node_ptr.getAttribute("Number"))
                    except ValueError:
                        utterance_id_val = None
                break
            node_ptr = node_ptr.parentNode
            
        # Additional utterance ID extraction: check for Utterance Number as direct child element
        if utterance_id_val is None and candidate.getElementsByTagName("UtteranceNumber"):
            utt_num_nodes = candidate.getElementsByTagName("UtteranceNumber")
            if utt_num_nodes and utt_num_nodes[0].childNodes:
                try:
                    utterance_id_val = int(utt_num_nodes[0].childNodes[0].data)
                except (ValueError, IndexError):
                    utterance_id_val = None

        safe_len_val = pos_len_val if pos_len_val is not None else len(phrase_text_val or "")

        return cls(
            cui=get_data(candidate, candidate_mapping['cui']),
            score=get_data(candidate, candidate_mapping['score']),
            matched=get_data(candidate, candidate_mapping['matched']),
            semtypes=[
                st.childNodes[0].data if st.childNodes else ""
                for st in candidate.getElementsByTagName(candidate_mapping['semtypes'])
                if st.childNodes and st.childNodes[0].nodeType == st.TEXT_NODE
            ],
            sources=sources,
            ismapping=is_mapping,
            isnegated=get_data(candidate, candidate_mapping['isnegated']),
            matchedstart=[int(m.childNodes[0].data) + 1 for m in candidate.getElementsByTagName("TextMatchStart")],
            matchedend=[int(m.childNodes[0].data) for m in candidate.getElementsByTagName("TextMatchEnd")],
            pos_start=pos_start_val,
            pos_length=pos_len_val,
            phrase_start=phrase_start_val,
            phrase_length=phrase_len_val,
            phrase_text=phrase_text_val,
            utterance_id=utterance_id_val,
        )

    def __str__(self):
        return "{0}, {1}, {2}, {3}, {4}, [{5}:{6}]".format(
            self.score, self.cui, self.semtypes, self.matched, self.isnegated, self.matchedstart, self.matchedend
        )

    # ------------------------------------------------------------------
    # Convenience coordinate helpers
    # ------------------------------------------------------------------

    @property
    def pos_start0(self):
        """0-based start position or ``None``."""
        if self.pos_start is None:
            return None
        return self.pos_start - 1  # stored value is 1-based

    @property
    def pos_end0(self):
        """0-based *exclusive* end = start0 + length."""
        if self.pos_start is None or self.pos_length is None:
            return None
        return (self.pos_start - 1) + self.pos_length

    # Handy for debugging equality between Java and Python outputs
    def span_tuple(self, zero_based: bool = True):
        if zero_based:
            return (self.pos_start0, self.pos_end0)
        return (self.pos_start, None if self.pos_length is None else self.pos_start + self.pos_length - 1)


class MMOS:
    def __init__(self, mmos):
        self.mmos = mmos
        self.index = 0

    def __iter__(self):
        return self

    def __next__(self):
        try:
            result = MMO(self.mmos[self.index])
        except IndexError:
            raise StopIteration
        self.index += 1
        return result


class MMO:
    def __init__(self, mmo):
        self.mmo = mmo
        self.index = 0

    def __iter__(self):
        for idx, tag in enumerate(["Candidates", "MappingCandidates"]):
            for candidates in self.mmo.getElementsByTagName(tag):
                candidates = candidates.getElementsByTagName("Candidate")
                for concept in candidates:
                    yield Concept.from_xml(concept, is_mapping=idx)


def parse(xml_file):
    document = parse_xml(xml_file).documentElement
    return MMOS(document.getElementsByTagName("MMO"))
