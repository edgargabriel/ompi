#ifndef MCA_FBTL_COMPOSIX_SZ_H
#define MCA_FBTL_COMPOSIX_SZ_H 1

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <errno.h>
#include <snappy-c.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <sys/uio.h>

#include "ompi/mca/fbtl/fbtl.h"

#include "fbtl_composix_sz_crc32.h"
#include "fbtl_composix_debug.h"
#define SNZ_FROM_LE32(x)  (x)

#define COMPRESSED_DATA_IDENTIFIER 0x00
#define UNCOMPRESSED_DATA_IDENTIFIER 0x01
#define PADDING_CHUNK_IDENTIFIER 0xfe
#define SKIPPABLE_CHUNK_LOWER 0x80
#define SKIPPABLE_CHUNK_UPPER 0xfd
#define UNSKIPPABLE_CHUNK_LOWER 0x02
#define UNSKIPPABLE_CHUNK_UPPER 0x7f


#define OFFSET_TYPE uint64_t
#define SIZE_TYPE uint64_t

static const unsigned char snappy_identifier[10] = {
	0xff, 
	0x06, 0x00, 0x00, 
	0x73, 0x4e, 0x61, 0x50, 0x70, 0x59
	};


#define MAX_CHUNK_HEADER_SIZE_VALUE 16777215 
#define MAX_CHUNK_LEN 16777215
#define MAX_UNCOMPRESSED_LEN 65536
#define MAX_CHUNK_UNCOMPRESSED_LEN 65536

#define ID_SIZE 1
#define CHUNK_LENGTH_SIZE 3
#define CRC32C_SIZE 4
#define CHUNK_HEADER_SIZE 4
#define CHUNK_METADATA_ALL_SIZE 8




int mca_fbtl_composix_sz_analyze_chunk(const char *chunk, char *id, SIZE_TYPE *chunk_size, unsigned int *checksum, SIZE_TYPE *uncompressed_length);

int mca_fbtl_composix_sz_compress(const char *uncompressed_data, size_t uncompressed_data_len, char *comp_data, size_t *comp_data_len, unsigned int *total_chunks);

SIZE_TYPE mca_fbtl_composix_sz_max_chunk_length(void);

SIZE_TYPE mca_fbtl_composix_sz_max_compressed_length(SIZE_TYPE uncomp_length);

size_t mca_fbtl_composix_sz_create_stream_identifier(char **stream_identifier);

int mca_fbtl_composix_sz_uncompress_chunk(const char *comp_chunk, char id, size_t data_len, size_t chunk_size, char *uncomp_chunk, size_t *uncomp_len);

char* mca_fbtl_composix_sz_create_annotation(struct iovec *compressed, struct iovec *uncompressed, int elements, unsigned int chunks, OFFSET_TYPE starting_offset, OFFSET_TYPE c_starting_offset, SIZE_TYPE *annotation_size);

int mca_fbtl_composix_sz_check_crc32c(const char *data, size_t datalen, const char *checksum);

SIZE_TYPE mca_fbtl_composix_sz_ann_size_bytes(void);

int mca_fbtl_composix_sz_analyze_ann(char *ann, char *type, OFFSET_TYPE *chunk_offset, SIZE_TYPE *chunk_size, OFFSET_TYPE *uncompressed_offset, SIZE_TYPE *uncompressed_size);

int mca_fbtl_composix_sz_search_ann(int ann_fd, unsigned int read_ahead, off_t iov_offset, struct iovec *iov_uc, struct iovec *iov_c, char *allocation_mask, size_t *deltas, off_t *sz_beginning, SIZE_TYPE *max_uncomp_len, unsigned int iovec_num);

int mca_fbtl_composix_sz_uncompress_chunk(const char *comp_chunk, char id, size_t data_len, size_t chunk_size, char *uncomp_chunk, size_t *uncomp_len);

int mca_fbtl_composix_sz_uncompress(char *comp, size_t comp_len, char *uncomp_buffer, size_t *uncomp_len);



#endif
