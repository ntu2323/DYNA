#!/usr/bin/env bash
#
# prepare_data_disk.sh — give the DYNA replica an ext4 working directory (run with sudo).
#
#   sudo OWNER=<user> ./prepare_data_disk.sh
#
# SplinterDB needs fallocate, which ext3 (the CloudLab Ubuntu image's root filesystem)
# lacks: replica startup asserts in clockcache_get_internal. This mounts an ext4
# filesystem at /mydata and creates /mydata/dyna, which start_dyna_replica.sh uses as
# its working directory when present.
#
# Idempotent. If /mydata is already a mountpoint it is reused (whatever its fs, as long
# as it is not ext3). Otherwise it formats the first (SSDs before HDDs) disk that is completely blank: no
# partitions, no filesystem signature, not mounted. It never touches a disk that has
# anything on it, and exits non-zero if no blank disk exists. The mount is not added to
# fstab (CloudLab experiments are ephemeral): re-run after a reboot.

set -euo pipefail
[[ $EUID -eq 0 ]] || { echo "run with sudo/root" >&2; exit 1; }
OWNER="${OWNER:-${SUDO_USER:-root}}"
MNT=/mydata

if mountpoint -q "$MNT"; then
    fstype=$(findmnt -n -o FSTYPE "$MNT")
    [[ "$fstype" != ext3 ]] || { echo "$MNT is ext3 (no fallocate)" >&2; exit 1; }
    echo "$MNT already mounted ($fstype)"
else
    disk=""
    # Non-rotational (SSD) disks first: c220g2 nodes have a blank HDD and a blank SSD.
    for d in $(lsblk -d -n -o NAME,TYPE,ROTA | awk '$2=="disk"{print $3, $1}' | sort -n -k1,1 -s | awk '{print $2}'); do
        dev="/dev/$d"
        [[ $(lsblk -n -o NAME "$dev" | wc -l) -eq 1 ]] || continue        # has partitions
        [[ -z "$(blkid -o value -s TYPE "$dev" 2>/dev/null || true)" ]] || continue
        wipefs -n "$dev" 2>/dev/null | grep -q . && continue              # any signature
        grep -q "^$dev" /proc/mounts && continue
        disk="$dev"
        break
    done
    [[ -n "$disk" ]] || { echo "no blank disk found; mount an ext4 fs at $MNT manually" >&2; exit 1; }
    echo "formatting blank $disk as ext4 at $MNT"
    mkfs.ext4 -q -F "$disk"
    mkdir -p "$MNT"
    mount "$disk" "$MNT"
fi
mkdir -p "$MNT/dyna"
chown "$OWNER" "$MNT/dyna"
df -hT "$MNT" | tail -1
