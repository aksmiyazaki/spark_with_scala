#!/usr/bin/env python
# -*- coding: utf-8 -*-

###### CONFIGURATIONS

# Original ssh keys used to daily stuff like git, etc.
original_ssh_keys = "/Users/alexandre.miyazaki/.ssh/true_keys"
# Standalone access keys without a passphrase, used to stup single node hadoop.
single_node_ssh_keys = "/Users/alexandre.miyazaki/.ssh/hadoop_standalone_keys"
# ssh dir
ssh_dir = "/Users/alexandre.miyazaki/.ssh"
################################

from shutil import copyfile
import sys

if sys.argv[1] == "1":
    print(f"Copying {single_node_ssh_keys}/* to {ssh_dir}")
    copyfile(f"{single_node_ssh_keys}/id_rsa", f"{ssh_dir}/id_rsa")
    copyfile(f"{single_node_ssh_keys}/id_rsa.pub", f"{ssh_dir}/id_rsa.pub")
else:
    print(f"Copying {original_ssh_keys}/* to {ssh_dir}")
    copyfile(f"{original_ssh_keys}/id_rsa", f"{ssh_dir}/id_rsa")
    copyfile(f"{original_ssh_keys}/id_rsa.pub", f"{ssh_dir}/id_rsa.pub")
