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
#include <sys/uio.h>
#include <limits.h>
#include "ompi/constants.h"
#include "ompi/mca/fbtl/fbtl.h"

static void free_c_iovec(struct iovec *i_c, int n)
{

    int i;
    for(i = 0; i < n; i++) {
    
        free((i_c[i].iov_base));
    }

}

ssize_t  mca_fbtl_composix_pwritev(ompio_file_t *fh )
{
    /*int *fp = NULL;*/
printf("\ncomposix\n");

	int ann_file = open("sz_ann", O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR);


    int i, block = 1, ret;
    struct iovec *iov = NULL;
    struct iovec *iov_c_0 = NULL;
    int iov_count = 0;
    OMPI_MPI_OFFSET_TYPE iov_offset = 0;
    ssize_t ret_code=0, bytes_written=0;
    struct flock lock;
    off_t total_length, end_offset=0;
    
    unsigned int total_chunks = 0;

    if (NULL == fh->f_io_array) {
        return OMPI_ERROR;
    }

    iov = (struct iovec *) malloc
        (OMPIO_IOVEC_INITIAL_SIZE * sizeof (struct iovec));
    iov_c_0 = (struct iovec *) malloc
        (OMPIO_IOVEC_INITIAL_SIZE * sizeof (struct iovec));

    if (NULL == iov) {
        opal_output(1, "OUT OF MEMORY\n");
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    
    if (NULL == iov_c_0) {
        opal_output(1, "OUT OF MEMORY\n");
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    
    
    SIZE_TYPE stream_id_len = sizeof(snappy_identifier);
    
    struct iovec *iov_c = iov_c_0 + 1;
    
    iov_c_0[0].iov_base = (char *) snappy_identifier;
    iov_c_0[0].iov_len = stream_id_len;
    
    
    //size_t mall_len = 0;
    char *comp_str = NULL;
    size_t comp_len = 0;
    size_t cumulative_len = stream_id_len;
    unsigned int chnks;
    int num_entries = fh->f_num_of_io_entries;
    for (i=0 ; i<fh->f_num_of_io_entries ; i++) {
	if (0 == iov_count) {
	    iov[iov_count].iov_base = fh->f_io_array[i].memory_address;
	    iov[iov_count].iov_len = fh->f_io_array[i].length;
printf("%i\n", i);
            compress_chunk(iov[iov_count].iov_base, 64, iov[iov_count].iov_len, &comp_str, &comp_len, &chnks);
            total_chunks += chnks;
         if (NULL == comp_str) {
		opal_output(1, "OUT OF MEMORY\n");
		return OMPI_ERR_OUT_OF_RESOURCE;
	    }
printf("cstr: \n");
	    iov_c[iov_count].iov_base = (void *) comp_str;
	    iov_c[iov_count].iov_len = comp_len;
            cumulative_len += comp_len;
	    iov_offset = (OMPI_MPI_OFFSET_TYPE)(intptr_t)fh->f_io_array[i].offset;
            end_offset = cumulative_len;
	    iov_count ++;
	}
	
	printf("iov %p\n", iov_c[iov_count].iov_base);

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
	
	
	if (OMPIO_IOVEC_INITIAL_SIZE*block <= iov_count + 1) {
	    block ++;
	    iov_c_0 = (struct iovec *)realloc
		(iov_c_0, OMPIO_IOVEC_INITIAL_SIZE * block *
		 sizeof(struct iovec));
		 
	    if (NULL == iov_c_0) {
		opal_output(1, "OUT OF MEMORY\n");
		return OMPI_ERR_OUT_OF_RESOURCE;
	    }
	    iov_c = iov_c + 1;
	}
	

	if (fh->f_num_of_io_entries != i+1) {
	    if ( (((OMPI_MPI_OFFSET_TYPE)(intptr_t)fh->f_io_array[i].offset +
		   (ptrdiff_t)fh->f_io_array[i].length) ==
		  (OMPI_MPI_OFFSET_TYPE)(intptr_t)fh->f_io_array[i+1].offset) &&
		 (iov_count < IOV_MAX )) {
		iov[iov_count].iov_base = fh->f_io_array[i+1].memory_address;
		iov[iov_count].iov_len  = fh->f_io_array[i+1].length;
/*
        mall_len = snappy_max_compressed_length(iov[iov_count].iov_len);
        comp_str = (char *) malloc(mall_len);
        if (NULL == comp_str) {
		opal_output(1, "OUT OF MEMORY\n");
		return OMPI_ERR_OUT_OF_RESOURCE;
	    }
*/
                compress_chunk(iov[iov_count].iov_base, 64, iov[iov_count].iov_len, &comp_str, &comp_len, &chnks);
                if (NULL == comp_str) {
                    opal_output(1, "OUT OF MEMORY\n");
                    return OMPI_ERR_OUT_OF_RESOURCE;
                }
                total_chunks += chnks;
	        iov_c[iov_count].iov_base = comp_str;
	        iov_c[iov_count].iov_len = comp_len;
		cumulative_len += comp_len;
                end_offset = cumulative_len;
                iov_count ++;
		continue;
	    }
	}
	/*
	  printf ("RANK: %d Entries: %d count: %d\n",
	  fh->f_rank,
	  fh->f_num_of_io_entries,
	  iov_count);
	  for (j=0 ; j<iov_count ; j++) {
	  printf ("%p %lld\n",
	  iov[j].iov_base,
	  iov[j].iov_len);
	  }

	*/
	end_offset = iov_offset + cumulative_len;
        total_length = (end_offset - (off_t)iov_offset);
        ret = mca_fbtl_composix_lock ( &lock, fh, F_WRLCK, iov_offset, total_length, OMPIO_LOCK_SELECTIVE ); 
        if ( 0 < ret ) {
            opal_output(1, "mca_fbtl_composix_pwritev: error in mca_fbtl_composix_lock() error ret=%d %s", ret, strerror(errno));
            free (iov);
            free_c_iovec(iov_c, num_entries);
	    free(iov_c_0); 
            /* just in case some part of the lock worked */
            mca_fbtl_composix_unlock ( &lock, fh );
            return OMPI_ERROR;
        }
        
    SIZE_TYPE ann_buffer_size;    
    char *annotation_buffer = create_annotation(iov_c, iov, iov_count, total_chunks, iov_offset, iov_offset + iov_c_0[0].iov_len, &ann_buffer_size);
    
#if defined (HAVE_PWRITEV) 
	ret_code = pwritev (fh->fd, iov_c_0, iov_count + 1, iov_offset);
	pwrite(ann_file, (void *) annotation_buffer, ann_buffer_size, 0);
#else
	if (-1 == lseek (fh->fd, iov_offset, SEEK_SET)) {
	    opal_output(1, "mca_fbtl_composix_pwritev: error in lseek:%s", strerror(errno));
            free(iov);
            free_c_iovec(iov_c, num_entries);
            free(iov_c_0);
            mca_fbtl_composix_unlock ( &lock, fh );
	    return OMPI_ERROR;
	}
	ret_code = writev (fh->fd, iov_c_0, iov_count + 1);
	write(ann_file, (void *) annotation_buffer, ann_buffer_size);
#endif
        mca_fbtl_composix_unlock ( &lock, fh );
	if ( 0 < ret_code ) {
	    bytes_written += ret_code;
	}
	else if (-1 == ret_code ) {
	    opal_output(1, "mca_fbtl_composix_pwritev: error in writev:%s", strerror(errno));
            free (iov);
            free_c_iovec(iov_c, num_entries);
            free(iov_c_0);
            return OMPI_ERROR;
	}
	iov_count = 0;
    }

    free (iov);
    free_c_iovec(iov_c, num_entries);
    free(iov_c_0);

    return bytes_written;
}
