/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2017 University of Houston. All rights reserved.
 * Copyright (c) 2015-2018 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "fbtl_composix.h"
#include "fbtl_composix_compression.h"
#include "mpi.h"
#include <unistd.h>
#include <limits.h>
#include "ompi/constants.h"
#include "ompi/mca/fbtl/fbtl.h"

#include "fbtl_composix_debug.h"

ssize_t mca_fbtl_composix_preadv (ompio_file_t *fh )
{
	/*int *fp = NULL;*/
	composix_debug_printf("composix\n");

	struct mca_fbtl_composix_info comp_inf;
	struct mca_fbtl_composix_info *composix_information = &comp_inf;

	composix_information->composix_format = MCA_FBTL_COMPOSIX_FORMAT_SZ;

	int i, block=1, ret;
	struct iovec *iov = NULL;
	struct iovec *iov_c = NULL; //vector for references of compressed chunks read from file
	int iov_count = 0;
	OMPI_MPI_OFFSET_TYPE iov_offset = 0;
	ssize_t bytes_read=0, ret_code=0;
	struct flock lock;
	off_t total_length, end_offset=0;

	int use_mask = 1;

	int ann_file = fh->annotation_fd;
	if(0 == ann_file) {
		//char *ext = ".ann";
		//strcat(fh->annotation_file_name, fh->f_filename);
		//strcat(fh->annotation_file_name, ext);
		ann_file = open(/*fh->annotation_file_name*/ "sz_ann",  O_RDONLY, S_IRUSR | S_IWUSR);
	}

	if (NULL == fh->f_io_array) {
		return OMPI_ERROR;
	}

	iov = (struct iovec *) malloc
		(OMPIO_IOVEC_INITIAL_SIZE * sizeof (struct iovec));
	if (NULL == iov) {
		opal_output(1, "OUT OF MEMORY\n");
		return OMPI_ERR_OUT_OF_RESOURCE;
	}

	for (i=0 ; i<fh->f_num_of_io_entries ; i++) {
		if (0 == iov_count) {
			iov[iov_count].iov_base = fh->f_io_array[i].memory_address;
			iov[iov_count].iov_len = fh->f_io_array[i].length;
			iov_offset = (OMPI_MPI_OFFSET_TYPE)(intptr_t)fh->f_io_array[i].offset;
			end_offset = (off_t)fh->f_io_array[i].offset + (off_t)fh->f_io_array[i].length;
			iov_count ++;
		}

		if (OMPIO_IOVEC_INITIAL_SIZE*block <= iov_count) {
			block ++;
			iov = (struct iovec *)realloc
				(iov, OMPIO_IOVEC_INITIAL_SIZE * block *
				 sizeof(struct iovec));
			if (NULL == iov) {
				opal_output(1, "OUT OF MEMORY\n");
				return OMPI_ERR_OUT_OF_RESOURCE;
			}
		}

		if (fh->f_num_of_io_entries != i+1) {
			if (((((OMPI_MPI_OFFSET_TYPE)(intptr_t)fh->f_io_array[i].offset +
								(ptrdiff_t)fh->f_io_array[i].length) ==
							(OMPI_MPI_OFFSET_TYPE)(intptr_t)fh->f_io_array[i+1].offset)) &&
					(iov_count < IOV_MAX ) ){
				iov[iov_count].iov_base =
					fh->f_io_array[i+1].memory_address;
				iov[iov_count].iov_len = fh->f_io_array[i+1].length;
				end_offset = (off_t)fh->f_io_array[i].offset + (off_t)fh->f_io_array[i].length;
				iov_count ++;
				continue;
			}
		}

		total_length = (end_offset - (off_t)iov_offset );

		iov_c = (struct iovec *) malloc
			(iov_count * sizeof (struct iovec));

		char *allocation_mask = malloc(iov_count); //element is 1 if corresponding element in iov_c has an iov_base which should be allocated, is 0 if it points to same base as iov
		size_t *deltas = malloc(iov_count*sizeof(size_t)); //amount of uncompressed buffer to not copy (when the full chunk data is not requested)
		off_t compressed_beginning_offset;
		SIZE_TYPE uncomp_buffer_size;
		if (NULL == iov_c || NULL == allocation_mask || NULL == deltas) {
			opal_output(1, "OUT OF MEMORY\n");
			return OMPI_ERR_OUT_OF_RESOURCE;
		}


		//search annotation file for locations and sizes of chunks to be read
		mca_fbtl_composix_search_annotation_file(ann_file, 10, iov_offset, iov, iov_c, allocation_mask, deltas, &compressed_beginning_offset, &uncomp_buffer_size, iov_count, composix_information);

		/* Allocate memory for compressed data which is bigger than the uncompressed data. 
		 * (if compressed data is smaller or equal to in size to the uncompressed data it
		 * will be stored in the iov base which the iov_c base will point to) */
		int i;
		for(i = 0; i < iov_count; i++) {
			if(1 == allocation_mask[i]) {
				iov_c[i].iov_base = (char *) malloc(iov_c[i].iov_len);
				if(NULL == iov_c[i].iov_base) {
					opal_output(1, "OUT OF MEMORY\n");
					free_c_iovec(iov_c, use_mask, allocation_mask, i+1);
					free(iov);
					free(iov_c);
					return OMPI_ERR_OUT_OF_RESOURCE;
				}
			}
		}


		ret = mca_fbtl_composix_lock ( &lock, fh, F_RDLCK, iov_offset, total_length, OMPIO_LOCK_SELECTIVE ); 
		if ( 0 < ret ) {
			opal_output(1, "mca_fbtl_composix_preadv: error in mca_fbtl_composix_lock() ret=%d: %s", ret, strerror(errno));
			free_c_iovec(iov_c, use_mask, allocation_mask, iov_count);
			free (iov);
			free(iov_c);
			/* Just in case some part of the lock worked */
			mca_fbtl_composix_unlock ( &lock, fh);
			return OMPI_ERROR;
		}
#if defined(HAVE_PREADV)
		ret_code = preadv (fh->fd, iov_c, iov_count, compressed_beginning_offset);
#else
		if (-1 == lseek (fh->fd, compressed_beginning_offset, SEEK_SET)) {
			opal_output(1, "mca_fbtl_composix_preadv: error in lseek:%s", strerror(errno));
			free_c_iovec(iov_c, use_mask, allocation_mask, iov_count);
			free(iov);
			free(iov_c);
			mca_fbtl_composix_unlock ( &lock, fh );
			return OMPI_ERROR;
		}
		ret_code = readv (fh->fd, iov_c, iov_count);
#endif
		mca_fbtl_composix_unlock ( &lock, fh );
		composix_debug_printf("c offset: %lu, u buff size: %lu\n", compressed_beginning_offset, uncomp_buffer_size);
		int iov_index;
		char *decompression_buffer = malloc(uncomp_buffer_size); //buffer to store decompressed data which can then be copied
		size_t uncomp_len;
		if (NULL == decompression_buffer) {
			opal_output(1, "OUT OF MEMORY\n");
			free_c_iovec(iov_c, use_mask, allocation_mask, iov_count);
			free(iov);
			free(iov_c);
			return OMPI_ERR_OUT_OF_RESOURCE;
		}
		composix_debug_printf("iov_count: %i\n", iov_count);


		if ( 0 < ret_code ) {
			bytes_read+=ret_code;
		}
		else if ( ret_code == -1 ) {
			opal_output(1, "mca_fbtl_composix_preadv: error in (p)readv:%s", strerror(errno));
			free_c_iovec(iov_c, use_mask, allocation_mask, iov_count);
			free(iov);
			free(iov_c);
			return OMPI_ERROR;
		}
		else if ( 0 == ret_code ){
			/* end of file reached, no point in continue reading; */
			break;
		}


		size_t copy_len;
		for(iov_index = 0; iov_index < iov_count; iov_index++) {
			composix_debug_printf("decompression begin\n");
			uncomp_len = uncomp_buffer_size;

			if(iov_c[iov_index].iov_len > 0) {

				//uncompress data
				mca_fbtl_composix_uncompress(iov_c[iov_index].iov_base, iov_c[iov_index].iov_len, decompression_buffer, &uncomp_len, composix_information);

				copy_len = iov[iov_index].iov_len;
				if(copy_len > uncomp_len)
				{
					copy_len = uncomp_len;
				}
				composix_debug_printf("decompression end uncomp_len: %lu %lu %lu\n", uncomp_len, copy_len, deltas[iov_index]);
				memcpy(iov[iov_index].iov_base, decompression_buffer + deltas[iov_index], copy_len); //retain only as much data as requested according to iov
				if(1 == allocation_mask[iov_index]) {
					free(iov_c[iov_index].iov_base); //free only those buffers which were allocated
					iov_c[iov_index].iov_base = NULL;
				}
			}
		}





		iov_count = 0;
	}

	free (iov);
	free(iov_c);
	return bytes_read;
}
