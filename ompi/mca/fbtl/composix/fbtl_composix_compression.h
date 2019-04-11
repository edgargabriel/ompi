#ifndef MCA_FBTL_COMPOSIX_COMPRESSION_H
#define MCA_FBTL_COMPOSIX_COMPRESSION_H 1

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

#include "fbtl_composix_debug.h"
#include "fbtl_composix_sz.h"


/* UNSIGNED INTEGER CODES FOR COMPRESSION FORMATS  */

#define MCA_FBTL_COMPOSIX_FORMAT_SZ 1

/* END OF COMPRESSION CODES */


/* STRUCT DEFINITIONS */

struct mca_fbtl_composix_info{

	/* GENERAL MEMBERS FOR COMPOSIX*/

	//which compression/decompression format to use
	unsigned int composix_format;

	//error returned from function call
	//0 means no error
	int composix_error;

	/* END OF GENERAL MEMBERS */



	/* FORMAT SPECIFIC MEMBERS */




	/* SZ MEMBERS */


	/* END OF SZ MEMBERS */



	/* END OF FORMAT SPECIFIC MEMBERS */

};

/* END OF STRUCT DEFINITIONS */



int mca_fbtl_composix_compress(const char *uncompressed_data, size_t uncompressed_data_len, char *comp_data, size_t *comp_data_len, unsigned int *total_chunks, struct mca_fbtl_composix_info *composix_info);

SIZE_TYPE mca_fbtl_composix_max_block_length(struct mca_fbtl_composix_info *composix_info);

SIZE_TYPE mca_fbtl_composix_max_compressed_length(size_t uncomp_len, struct mca_fbtl_composix_info *composix_info);

size_t mca_fbtl_composix_create_file_header(char **header, size_t *header_len, struct mca_fbtl_composix_info *composix_info);

char* mca_fbtl_composix_create_annotation(struct iovec *compressed, struct iovec *uncompressed, int elements, unsigned int chunks, OFFSET_TYPE starting_offset, OFFSET_TYPE c_starting_offset, SIZE_TYPE *annotation_size, struct mca_fbtl_composix_info *composix_info);

SIZE_TYPE mca_fbtl_composix_annotation_size_bytes(struct mca_fbtl_composix_info *composix_info);

int mca_fbtl_composix_search_annotation_file(int ann_fd, unsigned int read_ahead, off_t iov_offset, struct iovec *iov_uc, struct iovec *iov_c, char *allocation_mask, size_t *deltas, off_t *sz_beginning, SIZE_TYPE *max_uncomp_len, unsigned int iovec_num, struct mca_fbtl_composix_info *composix_info);

int mca_fbtl_composix_uncompress(char *comp, size_t comp_len, char *uncomp_buffer, size_t *uncomp_len, struct mca_fbtl_composix_info *composix_info);



#endif
