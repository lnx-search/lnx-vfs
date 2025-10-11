# Encryption Notes

We use `XChaCha20Poly1305`, `Blake3` and `Argon2ID` as our core algorithm and have the following responsibilities:

- `XChaCha20Poly1305` - Used for symmetric encryption of data.
- `Blake3` - Used for authenticating data that does not need to be encrypted, this is done via Blake3's keyed hash
  functionality.
- `Argon2ID` - Used for taking a users "key" (in reality, a password) and deriving the key which is intern used
  for encrypting & wrapping the master key. (See ["Encryption Keys"](#encryption-keys))

## Encryption Keys

The system generates a random master encryption key that is used to encrypt the data itself.
The user provided "key" is actually a password that is used to derive the encryption key that encrypts
the master encryption key. 

A user can rotate the provided key without having to re-encrypt all data on the system.

### Limitations

- If the master encryption key is leaked, there is no functionality to re-encrypt the data with a new
  master key. Although if this occurs your data has been compromised and nothing is saving you.

## Associated Data Specs

Each page for the various storage files has associated data tied into it preventing replay attacks, 
we define the current associated data for each file type here:

### Op Log (WAL)

- `file_id` - The unique file identifier.
- `log_file_id` - A random 64-bit ID regenerated on every log rotation.
- `sequence_id` - A monotonic ID assigned to each entry in the WAL. (Relative to the individual file.)
- `position_in_file` - The absolute start position of the block in the file.

### Page Table Checkpoint

- `file_id` - The unique file identifier.
- `target_page_file_id` - The unique ID of the page file this checkpoint is attached to.
- `num_changes` - The number of changes the checkpoint contains.
- `start_pos` - The starting position of the checkpoint data.

### Page Data

- `file_id` - The unique file identifier.
- `position_in_file` - The position of the data.
- `page_id` - The page ID within the page file.

* NOTE: The data in the page data file can only be decrypted with the information from the page table, an attacker
  cannot copy another block of data.

### File headers

File headers have their own associated data:

- `file_id` - The unique file identifier.
- `file_group` - The file group the file belongs to, i.e. WAL, Page Data, Checkpoints.
