#! /bin/bash

aws iam list-users
aws iam list-attached-user-policies --user-name=btrfs_experimental
aws iam get-policy --policy-arn=arn:aws:iam::096187466395:policy/btrfs_experimetal_dyn_policy

