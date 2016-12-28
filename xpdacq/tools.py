#!/usr/bin/env python
##############################################################################
#
# xpdacq            by Billinge Group
#                   Simon J. L. Billinge sb2896@columbia.edu
#                   (c) 2016 trustees of Columbia University in the City of
#                        New York.
#                   All rights reserved
#
# File coded by:    Timothy Liu
#
# See AUTHORS.txt for a list of people who contributed.
# See LICENSE.txt for license information.
#
##############################################################################
def regularize_dict_key(input_dict, target_chr, replace_chr):
    """recursively replace target character in keys with desired one

    Parameters
    ----------
    input_dict : dict
        a dictionary going to be cleaned. it can be nested
    target_chr : str
        character that will be replaced
    replace_chr : str
        character that is going to replace target character
    """
    for k, v in input_dict.items():
        if isinstance(v, dict):
            clean_k = k.replace(target_chr, replace_chr)
            input_dict[clean_k] = input_dict.pop(k)
            regularize_dict_key(v, target_chr, replace_chr)
        else:
            clean_k = k.replace(target_chr, replace_chr)
            input_dict[clean_k] = input_dict.pop(k)