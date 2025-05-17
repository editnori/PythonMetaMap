#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, absolute_import

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
    @classmethod
    def from_xml(cls, candidate, is_mapping=False):
        def get_data(elem, tag):
            return elem.getElementsByTagName(tag)[0].childNodes[0].data

        # Collect source vocabularies
        sources = []
        for src in candidate.getElementsByTagName("Source"):
            if src.childNodes and src.childNodes[0].nodeType == src.TEXT_NODE:
                sources.append(src.childNodes[0].data.strip())
        for srcs in candidate.getElementsByTagName("Sources"):
            if srcs.childNodes and srcs.childNodes[0].nodeType == srcs.TEXT_NODE:
                raw = srcs.childNodes[0].data.strip()
                for token in raw.replace("|", ":").replace(",", ":").split(":"):
                    token = token.strip()
                    if token:
                        sources.append(token)

        # Positional info
        pos_start_val = None
        pos_len_val = None
        pos_nodes = candidate.getElementsByTagName("PositionalInfo")
        if pos_nodes:
            node = pos_nodes[0]
            if node.childNodes:
                raw_text = node.childNodes[0].data.strip()
            else:
                attr_start = node.getAttribute("start") or node.getAttribute("Start")
                attr_len = node.getAttribute("length") or node.getAttribute("Length")
                raw_text = f"{attr_start}/{attr_len}" if attr_start and attr_len else ""
            if raw_text:
                tokens = []
                for chunk in raw_text.replace(";", " ").split():
                    chunk = chunk.strip()
                    if chunk and "/" in chunk:
                        tokens.append(chunk)
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
                        min_start_0 = min(starts)
                        pos_len_val = max(ends) - min_start_0
                        pos_start_val = min_start_0 + 1
                    except Exception:
                        pos_start_val = None
                        pos_len_val = None

        if pos_start_val is None:
            position_nodes = candidate.getElementsByTagName("Position")
            if position_nodes and position_nodes[0].hasAttribute("x") and position_nodes[0].hasAttribute("y"):
                try:
                    pos_start_val = int(position_nodes[0].getAttribute("x"))
                    pos_len_val = int(position_nodes[0].getAttribute("y"))
                except (ValueError, IndexError):
                    pass

        # Phrase-level position
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
            raw_phrase_pos = ""
            if p_pos_nodes:
                if p_pos_nodes[0].childNodes:
                    raw_phrase_pos = p_pos_nodes[0].childNodes[0].data.strip()
                else:
                    attr_start_p = p_pos_nodes[0].getAttribute("start") or p_pos_nodes[0].getAttribute("Start")
                    attr_len_p = p_pos_nodes[0].getAttribute("length") or p_pos_nodes[0].getAttribute("Length")
                    if attr_start_p and attr_len_p:
                        raw_phrase_pos = f"{attr_start_p}/{attr_len_p}"
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
                        min_start_0_p = min(starts)
                        phrase_len_val = max(ends) - min_start_0_p
                        phrase_start_val = min_start_0_p + 1
                    except Exception:
                        phrase_start_val = None
                        phrase_len_val = None

        phrase_text_val = None
        if phrase_node:
            phrase_text_nodes = phrase_node.getElementsByTagName("PhraseText")
            if phrase_text_nodes and phrase_text_nodes[0].childNodes:
                phrase_text_val = phrase_text_nodes[0].childNodes[0].data.strip()
            if not phrase_text_val and phrase_node.hasAttribute("text"):
                phrase_text_val = phrase_node.getAttribute("text").strip()
            if not phrase_text_val and phrase_node.childNodes:
                first_child = phrase_node.childNodes[0]
                if first_child.nodeType == first_child.TEXT_NODE:
                    phrase_text_val = first_child.data.strip()
        if not phrase_text_val:
            try:
                phrase_text_val = get_data(candidate, "CandidateMatched")
            except Exception:
                phrase_text_val = ""

        utterance_id_val = None
        node_ptr = candidate
        while node_ptr is not None and node_ptr.nodeType == node_ptr.ELEMENT_NODE:
            if node_ptr.tagName == "Utterance":
                if node_ptr.hasAttribute("id"):
                    utterance_id_val = int(node_ptr.getAttribute("id"))
                elif node_ptr.hasAttribute("Index") or node_ptr.hasAttribute("index"):
                    utterance_id_val = int(node_ptr.getAttribute("Index") or node_ptr.getAttribute("index"))
                elif node_ptr.hasAttribute("number") or node_ptr.hasAttribute("Number"):
                    utterance_id_val = int(node_ptr.getAttribute("number") or node_ptr.getAttribute("Number"))
                break
            node_ptr = node_ptr.parentNode
        if utterance_id_val is None and candidate.getElementsByTagName("UtteranceNumber"):
            utt_num_nodes = candidate.getElementsByTagName("UtteranceNumber")
            if utt_num_nodes and utt_num_nodes[0].childNodes:
                try:
                    utterance_id_val = int(utt_num_nodes[0].childNodes[0].data)
                except (ValueError, IndexError):
                    pass

        return cls(
            cui=get_data(candidate, candidate_mapping['cui']),
            score=get_data(candidate, candidate_mapping['score']),
            matched=get_data(candidate, candidate_mapping['matched']),
            semtypes=[st.childNodes[0].data for st in candidate.getElementsByTagName(candidate_mapping['semtypes'])],
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
                for concept in candidates.getElementsByTagName("Candidate"):
                    yield Concept.from_xml(concept, is_mapping=idx)


def parse(xml_file):
    document = parse_xml(xml_file).documentElement
    return MMOS(document.getElementsByTagName("MMO"))
