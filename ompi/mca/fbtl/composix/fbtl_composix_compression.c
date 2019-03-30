#include "fbtl_composix_compression.h"


int check_crc32c(const char *data, size_t datalen, const char *checksum)
{
  unsigned int actual_crc32c = masked_crc32c(data, datalen);
  unsigned int expected_crc32c = SNZ_FROM_LE32(*(unsigned int*)checksum);
  if (actual_crc32c != expected_crc32c) {
    printf("CRC32C error! (expected 0x%08x but 0x%08x)\n", expected_crc32c, actual_crc32c);
    return -1;
  }
  return 0;
}


SIZE_TYPE ann_size_bytes()
{

	return (SIZE_TYPE) ( sizeof(char) + 2*sizeof(OFFSET_TYPE) + 2*sizeof(SIZE_TYPE) );

}



int analyze_ann(char *ann, char *type, OFFSET_TYPE *chunk_offset, SIZE_TYPE *chunk_size, OFFSET_TYPE *uncompressed_offset, SIZE_TYPE *uncompressed_size)
{

	SIZE_TYPE i = 0;
	*type = (char) *(ann + i);
	i += sizeof(char);

	//*chunk_offset = (OFFSET_TYPE) (ann + i);
	memcpy(chunk_offset, (void *) (ann + i), sizeof(OFFSET_TYPE));
	i += sizeof(OFFSET_TYPE);
	//*chunk_size = (SIZE_TYPE) (ann + i);
	memcpy(chunk_size, (void *) (ann + i), sizeof(SIZE_TYPE));
	i += sizeof(SIZE_TYPE);

	//*uncompressed_offset = (OFFSET_TYPE) (ann + i);
	memcpy(uncompressed_offset, (void *) (ann + i), sizeof(OFFSET_TYPE));
        i += sizeof(OFFSET_TYPE);
        //*uncompressed_size = (SIZE_TYPE) (ann + i);
	memcpy(uncompressed_size, (void *) (ann + i), sizeof(SIZE_TYPE));
printf("ann i = %lu\n",i);
	return 0;

}


int search_ann(int ann_fd, unsigned int read_ahead, off_t iov_offset, struct iovec *iov_uc, struct iovec *iov_c, char *allocation_mask, size_t *deltas, off_t *sz_beginning, SIZE_TYPE *max_uncomp_len, unsigned int iovec_num)
{
	SIZE_TYPE size_per_ann = ann_size_bytes();
	SIZE_TYPE ann_buffer_size = size_per_ann*read_ahead;
	char *ann_buffer = (char *) malloc(ann_buffer_size);
	int ann_switch = 1;
	int found_first_chunk = 0;
	int found_last_chunk = 0;
	int searching_beginning = 1;
	int searching_end = 0;
	ssize_t bytes_read;
	SIZE_TYPE cumulative_len_uc = 0;
	SIZE_TYPE prev_cumulative_len_uc = 0;
	SIZE_TYPE cumulative_len_c = 0;
	OFFSET_TYPE ann_off = 0;
	OFFSET_TYPE ann_buffer_off = 0;
	int reduce_ann_buffer_off = 0;
	OFFSET_TYPE first_offset;

	bytes_read = pread(ann_fd, (void *) ann_buffer, ann_buffer_size, ann_off);
	ann_off += bytes_read;

	unsigned int iovec_index = 0; 

	char chunk_id;
	OFFSET_TYPE chunk_offset;
	SIZE_TYPE chunk_size;
	OFFSET_TYPE uncomp_offset;
	SIZE_TYPE uncomp_size;
	SIZE_TYPE comp_size;
	SIZE_TYPE max_uc_len = 0;

	for(iovec_index = 0; iovec_index < iovec_num; iovec_index++)
	{
		iov_c[iovec_index].iov_base = iov_uc[iovec_index].iov_base;
		iov_c[iovec_index].iov_len = 0;
		allocation_mask[iovec_index] = 0;
		max_uc_len = 1;
		first_offset = 0;
	}

	iovec_index = 0;
	while(bytes_read > 0 && ann_switch && iovec_index < iovec_num)
	{
		OFFSET_TYPE ann_buffer_off_add = size_per_ann;
		for(ann_buffer_off = 0; (!found_first_chunk || searching_beginning) && ann_buffer_off < (OFFSET_TYPE) bytes_read; ann_buffer_off += ann_buffer_off_add)
		{


			if(reduce_ann_buffer_off)
			{
				ann_buffer_off_add = size_per_ann;
				reduce_ann_buffer_off = 0;
			}

			if(!found_first_chunk)
			{
				analyze_ann(ann_buffer + ann_buffer_off, &chunk_id, &chunk_offset, &chunk_size, &uncomp_offset, &uncomp_size);
				comp_size = chunk_size + ID_SIZE + CHUNK_LENGTH_SIZE;
				printf("ann_buffer_off: %lu bytes_read: %lu size_per_ann: %lu c_off: %lu c_size %lu u_off %lu u_size %lu", ann_buffer_off, bytes_read, size_per_ann, chunk_offset, comp_size, uncomp_offset, uncomp_size);
				if(!found_first_chunk && uncomp_offset + uncomp_size > iov_offset)
				{
					found_first_chunk = 1;
					//*sz_beginning = (off_t) chunk_offset;
					first_offset = chunk_offset;
					deltas[iovec_index] = iov_offset - uncomp_offset;
				}

				if(found_first_chunk && searching_beginning)
				{
					//prev_cumulative_len_uc = cumulative_len_uc;
					cumulative_len_uc += uncomp_size;
					cumulative_len_c += comp_size;
                                        if(iov_uc[iovec_index].iov_len <= cumulative_len_uc)
                                        {
						searching_beginning = 0;
						iov_c[iovec_index].iov_len = cumulative_len_c;
						if(iovec_index > 0) {
							deltas[iovec_index] = iov_uc[iovec_index - 1].iov_len + deltas[iovec_index - 1] - prev_cumulative_len_uc;
						}
						if(cumulative_len_uc > max_uc_len) {
							max_uc_len = cumulative_len_uc;
						}
						if(iov_uc[iovec_index].iov_len >= cumulative_len_c)
						{
							iov_c[iovec_index].iov_base = iov_uc[iovec_index].iov_base;
							allocation_mask[iovec_index] = 0;
						}
						else
						{
							//iov_c[iovec_index].iov_base = malloc(cumulative_len_c);
							allocation_mask[iovec_index] = 1;
						}
						ann_buffer_off_add = 0;
						reduce_ann_buffer_off = 1;
						iovec_index++;
						cumulative_len_uc = 0;
						cumulative_len_c = 0;
						if(iovec_index < iovec_num) {
							searching_beginning = 1;
						}

						prev_cumulative_len_uc = cumulative_len_uc - uncomp_size;
                                        }
					else {

                                                iov_c[iovec_index].iov_len = cumulative_len_c;
                                                if(iovec_index > 0) {
                                                        deltas[iovec_index] = iov_uc[iovec_index - 1].iov_len + deltas[iovec_index - 1] - prev_cumulative_len_uc;
                                                }
                                                if(cumulative_len_uc > max_uc_len) {
                                                        max_uc_len = cumulative_len_uc;
                                                }
                                                if(iov_uc[iovec_index].iov_len >= cumulative_len_c)
                                                {
                                                        iov_c[iovec_index].iov_base = iov_uc[iovec_index].iov_base;
                                                        allocation_mask[iovec_index] = 0;
                                                }
                                                else
                                                {
                                                        //iov_c[iovec_index].iov_base = malloc(cumulative_len_c);
                                                        allocation_mask[iovec_index] = 1;
                                                }


					}


				}
			}

			printf("max: %lu\n", max_uc_len);

		}

		bytes_read = pread(ann_fd, (void *) ann_buffer, ann_buffer_size, ann_off);
        	ann_off += bytes_read;
		ann_buffer_off = 0;
		if(!searching_beginning)
		{
			searching_beginning = 1;
			cumulative_len_uc = 0;
			cumulative_len_uc = 0;

		}
	}


        for(iovec_index = 0; iovec_index < iovec_num; iovec_index++)
        {
printf("delta: %lu c_len: %lu\n", deltas[iovec_index], iov_c[iovec_index].iov_len);
                if(1 == allocation_mask[iovec_index]) {
			iov_c[iovec_index].iov_base = malloc(iov_c[iovec_index].iov_len);
		}
		else {
			iov_c[iovec_index].iov_base = iov_uc[iovec_index].iov_base;
		}
                
        }


	*sz_beginning = (off_t) first_offset;
	*max_uncomp_len = max_uc_len;

	return 0;


}




size_t create_stream_identifier(char **stream_identifier)
{
	size_t szid = sizeof(snappy_identifier);
  char *strm_id = calloc(sizeof(snappy_identifier), 1);
  memcpy(strm_id,snappy_identifier, sizeof(snappy_identifier));
  *stream_identifier = strm_id;
  return szid;
}


int compress_chunk(const char *uncompressed_data, size_t block_size, size_t uncompressed_data_len, char **comp_data, size_t *comp_data_len, unsigned int *total_chunks)
{
printf("compress_chunk\n");
  char *compressed_data;
  const size_t max_uncompressed_data_len = MAX_UNCOMPRESSED_LEN;
  const size_t max_compressed_data_len = snappy_max_compressed_length(max_uncompressed_data_len);
  const size_t full_max_compressed_data_len = snappy_max_compressed_length(uncompressed_data_len);

  const size_t type_code_size = ID_SIZE;
  const size_t chunk_len_size = CHUNK_LENGTH_SIZE;
  const size_t crc32c_size = CRC32C_SIZE;
  const size_t total_header_size = type_code_size + chunk_len_size;
  const size_t total_metadata_size = total_header_size + crc32c_size;
  size_t compressed_data_len = full_max_compressed_data_len + 2*total_metadata_size + total_metadata_size*( (uncompressed_data_len/max_uncompressed_data_len) );
printf("%lu %lu", max_compressed_data_len, compressed_data_len); 
SIZE_TYPE compressed_data_len_return = compressed_data_len;
SIZE_TYPE comp_len_cumulative = 0;
  char type_code;
  size_t write_len;

  compressed_data = calloc(compressed_data_len, 1);

  unsigned int crc32c;
  int err = 1;

  if (NULL == uncompressed_data || NULL == compressed_data) {
    printf("out of memory\n");
  }

  unsigned int t_chunks = 0;
  SIZE_TYPE full_comp_len = 0;
  SIZE_TYPE remaining_len = uncompressed_data_len;
  SIZE_TYPE uncomp_chunk_len;
  OFFSET_TYPE u_buffer_offset = 0;
  OFFSET_TYPE c_buffer_offset = 0;
  unsigned int compress_switch = 1;
  
  while(compress_switch) {
printf("compress_switch\n");	  
	  if(remaining_len > max_uncompressed_data_len)
	  {
		  uncomp_chunk_len = max_uncompressed_data_len;
		  remaining_len -= max_uncompressed_data_len;
	  }
	  else
	  {
		  uncomp_chunk_len = remaining_len;
		  compress_switch = 0;
	  }
  
  crc32c = masked_crc32c(uncompressed_data + u_buffer_offset, uncomp_chunk_len);
  snappy_compress(uncompressed_data + u_buffer_offset, uncomp_chunk_len, compressed_data + total_metadata_size + c_buffer_offset, &compressed_data_len_return);

  SIZE_TYPE full_str_len;
  if(compressed_data_len_return < uncomp_chunk_len) {

  full_str_len = total_metadata_size + compressed_data_len_return;

  type_code = COMPRESSED_DATA_IDENTIFIER;
  write_len = compressed_data_len_return + crc32c_size;

  }
  else
  {
	   full_str_len = total_metadata_size + uncomp_chunk_len;
	   memcpy(compressed_data + total_metadata_size + c_buffer_offset, uncompressed_data + u_buffer_offset, uncomp_chunk_len);
       type_code = UNCOMPRESSED_DATA_IDENTIFIER;
       write_len = uncomp_chunk_len + crc32c_size;  
  }
  
  comp_len_cumulative += write_len;
  compressed_data_len_return = compressed_data_len - comp_len_cumulative;
  
  memcpy(compressed_data + c_buffer_offset, &type_code, type_code_size);
  memcpy(compressed_data + c_buffer_offset + type_code_size, &write_len, chunk_len_size);
  memcpy(compressed_data + c_buffer_offset + type_code_size + chunk_len_size, &crc32c, crc32c_size);
  
  
  u_buffer_offset += uncomp_chunk_len;
  c_buffer_offset += full_str_len;
  full_comp_len += full_str_len;
  t_chunks++;
  }

  err = 0;

  *comp_data_len = full_comp_len;
  *total_chunks = t_chunks;
  *comp_data = compressed_data;
  printf("comp:\n");
  printf("comp addr:");
  

	//printf("%s", comp_data);
	//printf("%s", uncompressed_data);

  return err;

}


int uncompress_chunk(const char *comp_chunk, char id, size_t data_len, size_t chunk_size, char *uncomp_chunk, size_t *uncomp_len)
{
  const size_t max_data_len = MAX_CHUNK_LEN;
  const size_t max_uncompressed_data_len = MAX_UNCOMPRESSED_LEN;
  size_t uncompressed_data_len = max_uncompressed_data_len;
  char *uncompressed_data = uncomp_chunk;
  int err = 1;
  size_t bytesread = 0;
  size_t un_off;



  ssize_t byteswrt = 0;
  if(data_len == 0)
    {
      return 0;
    }

  if (id == COMPRESSED_DATA_IDENTIFIER) {

    if (data_len < 4) {
      printf("too short data length %lu\n", data_len);

    }
    snappy_uncompress(comp_chunk + (data_len - chunk_size) + 4, chunk_size - 4, uncompressed_data, &uncompressed_data_len);
    if (check_crc32c(uncompressed_data, uncompressed_data_len, comp_chunk + (data_len - chunk_size)) != 0) {
      printf("Checksum error.\n");

    }


  } else if (id == UNCOMPRESSED_DATA_IDENTIFIER) {

    if (data_len < 4) {
      printf("too short data length %lu\n", data_len);

    }
    memcpy(uncompressed_data, comp_chunk + (data_len - chunk_size) + 4, chunk_size - 4);
    uncompressed_data_len = chunk_size - 4;
  } else if (id < 0x80) {

    printf("Unsupported identifier 0x%02x\n", id);

  } else {

    printf("Not writing skippable chunk.\n");
  }

printf("done uncomp\n");
  *uncomp_len = uncompressed_data_len;
  uncomp_chunk = uncompressed_data;
  err = 0;

  return err;
}


int uncompress_v(char *comp, size_t comp_len, char *uncomp_buffer, size_t *uncomp_len)
{

	const char *comp_str = (const char *) comp;
	SIZE_TYPE total_comp_len = 0;
	SIZE_TYPE partial_comp_len = 0;
	SIZE_TYPE total_uncomp_len = 0;
	SIZE_TYPE partial_uncomp_len = 0;
	SIZE_TYPE u_buff_size = *uncomp_len;

	OFFSET_TYPE i = 0;
	OFFSET_TYPE j = 0;
	int uncompress_switch = 1;

	char id;
	SIZE_TYPE data_len;
	SIZE_TYPE chunk_size;
	//SIZE_TYPE partial_uncomp_len;
	unsigned int checksum;

	char *current_comp = comp;
	char *current_uncomp = uncomp_buffer;

	int snappy_return;

	while(uncompress_switch) {

		current_comp += i;
		current_uncomp += j;
		analyze_chunk(current_comp, &id, &chunk_size, &checksum, &partial_uncomp_len);
		partial_comp_len = chunk_size + ID_SIZE + CHUNK_LENGTH_SIZE;
		total_comp_len += partial_comp_len;
		snappy_return = uncompress_chunk(current_comp, id, partial_comp_len, chunk_size, current_uncomp, &partial_uncomp_len);
		total_uncomp_len += partial_uncomp_len;
		i = partial_comp_len;
		j = partial_uncomp_len;

		if(total_comp_len >= comp_len)
		{
			uncompress_switch = 0;
		}
		//printf("uncomp_v:return code: %i %lu %s\n", snappy_return, total_comp_len, current_uncomp);

	}	

	*uncomp_len = total_uncomp_len;
	return 0;

}


int analyze_chunk(const char *chunk, char *id, SIZE_TYPE *chunk_size, unsigned int *checksum, SIZE_TYPE *uncompressed_length)
{

  *id = *chunk;
  SIZE_TYPE total_size;
  if(*id <= UNSKIPPABLE_CHUNK_LOWER || *id >= UNSKIPPABLE_CHUNK_UPPER) {
  
    memcpy((void *) chunk_size, (const void*)(chunk + ID_SIZE), (size_t) CHUNK_LENGTH_SIZE );
    if(*id == COMPRESSED_DATA_IDENTIFIER || *id == UNCOMPRESSED_DATA_IDENTIFIER) {
      memcpy((void *) checksum, (const void*)(chunk + ID_SIZE + CHUNK_LENGTH_SIZE), (size_t) CRC32C_SIZE);
      total_size = ID_SIZE + CHUNK_LENGTH_SIZE + *chunk_size;
      if(*id == COMPRESSED_DATA_IDENTIFIER) {
        snappy_uncompressed_length(chunk + ID_SIZE + CHUNK_LENGTH_SIZE + CRC32C_SIZE, (size_t) total_size, (size_t *) uncompressed_length);
      } 
      else {
        *uncompressed_length = *chunk_size - (SIZE_TYPE) CRC32C_SIZE;
      }
    }
  
  }
  return 0;
}


char* create_annotation(struct iovec *compressed, struct iovec *uncompressed, int elements, unsigned int chunks, OFFSET_TYPE starting_offset, OFFSET_TYPE c_starting_offset, SIZE_TYPE *annotation_size) {
	
	printf("create_ann\n");
	
  const size_t type_code_size = ID_SIZE;
  const size_t chunk_len_size = CHUNK_LENGTH_SIZE;
  const size_t crc32c_size = CRC32C_SIZE;
  const size_t total_header_size = type_code_size + chunk_len_size;
  const size_t header_metadata_size = total_header_size;
  const size_t total_metadata_size = total_header_size + crc32c_size;
	
  SIZE_TYPE ann_size = (SIZE_TYPE) ( sizeof(char) + 2*sizeof(OFFSET_TYPE) + 2*sizeof(SIZE_TYPE) );
  *annotation_size = ann_size*chunks;
  char *annotation = (char *) malloc(*annotation_size);
  if(NULL == annotation)
  {
	  printf("out of memory\n");
	  return NULL;
  }
  SIZE_TYPE ann_offset = 0;
  SIZE_TYPE ann_partial = 0;
  OFFSET_TYPE current_offset = c_starting_offset;
  OFFSET_TYPE uncomp_current_offset = starting_offset;
  int i;
  
  char c_id; 
  SIZE_TYPE c_size;
  unsigned int c_checks;
  SIZE_TYPE c_uncomp_len;
  unsigned int chunks_left;
  OFFSET_TYPE per_element_offset;
  SIZE_TYPE c_len;
  char *c_base;
  
  for(i = 0; i < elements; i++)
  {
printf("create ann i = %i\n", i);  
	  chunks_left = 1;
	  per_element_offset = 0;
	  c_len = compressed[i].iov_len;
	  c_base = (char *) compressed[i].iov_base;
	  
	  while(chunks_left) {
	  
	  analyze_chunk(c_base + per_element_offset, &c_id, &c_size, &c_checks, &c_uncomp_len);
	  ann_partial = type_code_size;
	  memcpy(annotation+ann_offset, &c_id, ann_partial);
	  ann_offset += ann_partial;
	  
	  ann_partial = sizeof(current_offset);
	  memcpy(annotation+ann_offset, &current_offset, ann_partial);
	  ann_offset += ann_partial;
	  current_offset += total_metadata_size - crc32c_size + c_size;
	  
	  ann_partial = sizeof(c_size);
	  memcpy(annotation+ann_offset, &c_size, ann_partial);
	  ann_offset += ann_partial;
	  
	  ann_partial = sizeof(uncomp_current_offset);
	  memcpy(annotation+ann_offset, &uncomp_current_offset, ann_partial);
	  ann_offset += ann_partial;
	  uncomp_current_offset += uncompressed[i].iov_len;
	  
	  ann_partial = sizeof(c_uncomp_len);
	  memcpy(annotation+ann_offset, &c_uncomp_len, ann_partial);
	  ann_offset += ann_partial;
	  
	  if(c_len > header_metadata_size + c_size) {
		  per_element_offset += header_metadata_size + c_size;
		  c_len -= header_metadata_size + c_size;
	  }
	  else {
		  chunks_left = 0;
	  }
printf("chunks left = %i %lu %lu %lu\n", chunks_left, c_len, header_metadata_size + c_size, per_element_offset); 	  
      }
	  
  }

  return annotation;


}

