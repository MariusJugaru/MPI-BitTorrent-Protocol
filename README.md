# MPI BitTorrent Protocol

## Overview
This project impleBitTorrent is a peer-to-peer (P2P) file-sharing protocol that enables decentralized distribution of files over the Internet.
Instead of downloading from a central server, users (peers) share segments of a file among themselves, increasing efficiency and speed.

## How it works
- Files are split into **segments**, which are shared among **peers**.
- Each **peer** downloads missing segments and uploads available ones.
- A **tracker** helps peers find each other but does not store any file data.

## Roles in BitTorrent
- **Seed**: Has the complete file and only uploads.
- **Peer**: Downloads and uploads segments simultaneously.
- **Leecher**: Only downloads without contributing back.