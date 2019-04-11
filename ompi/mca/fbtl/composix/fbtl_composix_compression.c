#include "fbtl_composix_compression.h"




size_t mca_fbtl_composix_max_block_length(struct mca_fbtl_composix_info *composix_info)
{
	unsigned int comp_format = composix_info->composix_format;
	size_t return_value;
	int error_code = 0;

	if(MCA_FBTL_COMPOSIX_FORMAT_SZ == comp_format)	{
		return_value = mca_fbtl_composix_sz_max_chunk_length();
	}

	composix_info->composix_error = error_code;
	return return_value;
}


size_t mca_fbtl_composix_max_compressed_length(size_t uncomp_len, struct mca_fbtl_composix_info *composix_info)
{

	unsigned int comp_format = composix_info->composix_format;
	size_t return_value;
	int error_code = 0;

	if(MCA_FBTL_COMPOSIX_FORMAT_SZ == comp_format) {
		return_value = mca_fbtl_composix_sz_max_compressed_length(uncomp_len);
	}

	composix_info->composix_error = error_code;
	return return_value;

}

size_t mca_fbtl_composix_annotation_size_bytes(struct mca_fbtl_composix_info *composix_info)
{

	unsigned int comp_format = composix_info->composix_format;
	size_t return_value;
	int error_code = 0;

	if(MCA_FBTL_COMPOSIX_FORMAT_SZ == comp_format) {
		return_value = mca_fbtl_composix_sz_ann_size_bytes();
	}

	composix_info->composix_error = error_code;
	return return_value;


}




int mca_fbtl_composix_search_annotation_file(int ann_fd, unsigned int read_ahead, off_t iov_offset, struct iovec *iov_uc, struct iovec *iov_c, char *allocation_mask, size_t *deltas, off_t *sz_beginning, SIZE_TYPE *max_uncomp_len, unsigned int iovec_num, struct mca_fbtl_composix_info *composix_info)
{

	unsigned int comp_format = composix_info->composix_format;
	size_t return_value;
	int error_code = 0;

	if(MCA_FBTL_COMPOSIX_FORMAT_SZ == comp_format)	{
		return_value = mca_fbtl_composix_sz_search_ann(ann_fd, read_ahead, iov_offset, iov_uc, iov_c, allocation_mask, deltas, sz_beginning, max_uncomp_len, iovec_num);
	}

	composix_info->composix_error = error_code;
	return return_value;

}

size_t mca_fbtl_composix_create_file_header(char **header, size_t *header_len, struct mca_fbtl_composix_info *composix_info)
{
	unsigned int comp_format = composix_info->composix_format;
	size_t return_value;
	int error_code = 0;

	if(MCA_FBTL_COMPOSIX_FORMAT_SZ == comp_format)	{
		return_value = mca_fbtl_composix_sz_create_stream_identifier(header);
	}

	composix_info->composix_error = error_code;
	return return_value;
}


int mca_fbtl_composix_compress(const char *uncompressed_data, size_t uncompressed_data_len, char *comp_data, size_t *comp_data_len, unsigned int *total_blocks, struct mca_fbtl_composix_info *composix_info)
{
	unsigned int comp_format = composix_info->composix_format;
	size_t return_value;
	int error_code = 0;

	if(MCA_FBTL_COMPOSIX_FORMAT_SZ == comp_format)	{
		return_value = mca_fbtl_composix_sz_compress(uncompressed_data, uncompressed_data_len, comp_data, comp_data_len, total_blocks);
	}

	composix_info->composix_error = error_code;
	return return_value;
}


int mca_fbtl_composix_uncompress(char *compressed_data, size_t compressed_len, char *uncompressed_data, size_t *uncompressed_len, struct mca_fbtl_composix_info *composix_info)
{
	unsigned int comp_format = composix_info->composix_format;
	size_t return_value;
	int error_code = 0;

	if(MCA_FBTL_COMPOSIX_FORMAT_SZ == comp_format)	{
		return_value = mca_fbtl_composix_sz_uncompress(compressed_data, compressed_len, uncompressed_data, uncompressed_len);
	}

	composix_info->composix_error = error_code;
	return return_value;
}


char* mca_fbtl_composix_create_annotation(struct iovec *compressed, struct iovec *uncompressed, int elements, unsigned int chunks, OFFSET_TYPE starting_offset, OFFSET_TYPE c_starting_offset, SIZE_TYPE *annotation_size, struct mca_fbtl_composix_info *composix_info)
{

	unsigned int comp_format = composix_info->composix_format;
	char *return_value;
	int error_code = 0;

	if(MCA_FBTL_COMPOSIX_FORMAT_SZ == comp_format)	{
		return_value = mca_fbtl_composix_sz_create_annotation(compressed, uncompressed, elements, chunks, starting_offset, c_starting_offset, annotation_size);
	}

	composix_info->composix_error = error_code;
	return return_value;

}

