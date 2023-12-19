/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2024      Advanced Micro Devices, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"
#include "fbtl_posix.h"

#include "mpi.h"
#include <unistd.h>
#include <limits.h>
#include "ompi/constants.h"
#include "ompi/mca/fbtl/fbtl.h"

#if defined (FBTL_POSIX_HAVE_IO_URING)
mca_fbtl_posix_io_uring_regmem_t mca_fbtl_posix_io_uring_data;
#endif

/**
 * Returns the position of the buffer pointer in the iovec
 * or -1 if the buffer is not registered
 */
int mca_fbtl_posix_get_registration_id(ompio_file_t *fh, const void *buf, size_t size)
{
#if defined (FBTL_POSIX_HAVE_IO_URING)
    mca_fbtl_posix_io_uring_regmem_t *reg = &mca_fbtl_posix_io_uring_data;
    int i;

    if (0 == reg->nelem) {
        return -1;
    }

    for (i = 0; i < reg->nelem; i++) {
        if (((char*)buf >= (char*)reg->iov[i].iov_base)                        &&
            ((char*)buf < ((char*)reg->iov[i].iov_base + reg->iov[i].iov_len)) &&
            (((char*)buf + size) <= ((char*)reg->iov[i].iov_base + reg->iov[i].iov_len))) {
            return i;
        }
    }
#endif
    return -1;
}

int mca_fbtl_posix_register_buffers(ompio_file_t *fh, struct iovec *iov, int nelem)
{
#if defined (FBTL_POSIX_HAVE_IO_URING)
    mca_fbtl_posix_io_uring_regmem_t *reg = &mca_fbtl_posix_io_uring_data;
    int i;

    if (!mca_fbtl_posix_enable_io_uring){
        /* Don't register buffers if io_uring support has been disabled by user */
        return OPAL_SUCCESS;
    }

    if (reg->nelem > 0) {
        bool need_to_unreg=false;
        int id;

        for (i=0; i<nelem; i++) {
            id = mca_fbtl_posix_get_registration_id(fh, iov[i].iov_base, iov[i].iov_len);
            if (id < 0) {
                need_to_unreg = true;
                break;
            }
        }

        if (need_to_unreg) {
            io_uring_unregister_buffers(&reg->ring);
        } else {
            return OPAL_SUCCESS;
        }
    }

    if (reg->nelem == (reg->maxelem - 1)) {
        // double maxelem and allocate iov-array of new size
        // copy elements over from old iov array to new one
        printf("reg->nelem too small: you should probably implement this \n");
    }

    for (i=0; i<nelem; i++) {
        reg->iov[reg->nelem].iov_base = iov[i].iov_base;
        reg->iov[reg->nelem].iov_len  = iov[i].iov_len;
        reg->nelem++;
    }

    int ret = io_uring_register_buffers(&reg->ring, reg->iov, reg->nelem);
    if (ret < 0) {
        opal_output(1, "mca_fbtl_posix_register_buffers: buffer registration failed");
        return OPAL_ERROR;
    }
#endif

    return OPAL_SUCCESS;
}

void mca_fbtl_posix_unregister_all_buffers (ompio_file_t *fh)
{
    /* Empty function. 
    **
    ** The io_uring buffer_unregister functions unregisters *all*
    ** buffers associated with an io_uring ring, and is very slow.
    ** In addition, since the fixed buffers are allocated/used 
    ** in common/ompio, the buffers might be still in use when a
    ** file is closed, e.g. from another file.
    **
    ** For io_uring we will unregister the buffers in component finalize.
    */
    return;
}

/* The fixed interfaces are only intended to be used for temporary buffers e.g. 
** aggregators in collective I/O or individual I/O that requires staging buffers 
** (e.g. external32, GPU buffers, etc.).
*/

ssize_t mca_fbtl_posix_iouring_post_fixed (ompio_file_t *fh, int buf_index, ompi_request_t *request, int type)
{
#if defined (FBTL_POSIX_HAVE_IO_URING)
    struct io_uring_sqe *sqe;
    mca_ompio_request_t *req = (mca_ompio_request_t *) request;
    mca_fbtl_posix_request_data_t *data=NULL;
    mca_fbtl_posix_io_uring_regmem_t *reg = &mca_fbtl_posix_io_uring_data;
    off_t start_offset, end_offset, total_length;
    int i, num_slots;
    int ret = OMPI_SUCCESS;

    data = (mca_fbtl_posix_request_data_t *) malloc ( sizeof (mca_fbtl_posix_request_data_t));
    if ( NULL == data ) {
        opal_output (1,"mca_fbtl_posix_iouring_post_fixed: could not allocate memory\n");
        return 0;
    }

    assert ((FBTL_POSIX_IO_URING_WRITE_FIXED == type ||
             FBTL_POSIX_IO_URING_READ_FIXED  == type));

    data->prd_req_count      = fh->f_num_of_io_entries;
    data->prd_open_reqs      = fh->f_num_of_io_entries;
    data->prd_req_type       = type; 

    data->prd_req_chunks     = -1; //unused with io_uring
    data->prd_total_len      = 0;
    data->prd_lock_counter   = 0;
    data->prd_fh             = fh;

    data->prd_iou.iou_ring   = &reg->ring;
    data->prd_iou.iou_iov    = (struct iovec *)malloc (sizeof(struct iovec) * fh->f_num_of_io_entries);
    data->prd_iou.iou_offset = (off_t *) malloc (sizeof(off_t) * fh->f_num_of_io_entries);
    if (NULL == data->prd_iou.iou_iov || NULL == data->prd_iou.iou_offset) {
        opal_output(1,"mca_fbtl_posix_iouring_post_fixed: could not allocate memory");
        ret = OPAL_ERROR;
        goto error_exit;
    }

    for (i=0; i<fh->f_num_of_io_entries; i++ ) {
        data->prd_iou.iou_iov[0].iov_base = fh->f_io_array[i].memory_address;
        data->prd_iou.iou_iov[0].iov_len  = fh->f_io_array[i].length;
        data->prd_iou.iou_offset[0]       = (OMPI_MPI_OFFSET_TYPE)(intptr_t)fh->f_io_array[i].offset;
        data->prd_iou.iou_req_status      = 0;
    }

    data->prd_first_active_req = 0;

    num_slots = io_uring_sq_space_left(&reg->ring);
    data->prd_last_active_req  = num_slots < fh->f_num_of_io_entries ? num_slots : fh->f_num_of_io_entries;

    start_offset = data->prd_iou.iou_offset[data->prd_first_active_req];
    end_offset   = data->prd_iou.iou_offset[data->prd_last_active_req-1] +
                   data->prd_iou.iou_iov[data->prd_last_active_req-1].iov_len;
    total_length = (end_offset - start_offset);
    ret = mca_fbtl_posix_lock(&data->prd_lock, data->prd_fh,
                              (type == FBTL_POSIX_IO_URING_WRITE_FIXED ? F_WRLCK : F_RDLCK),
                              start_offset, total_length, OMPIO_LOCK_ENTIRE_REGION,
                              &data->prd_lock_counter);
    if ( 0 < ret ) {
        opal_output(1, "mca_fbtl_posix_iouring_post_fixed: error in mca_fbtl_posix_lock() "
                    "error ret=%d %s", ret, strerror(errno));
        mca_fbtl_posix_unlock ( &data->prd_lock, data->prd_fh, &data->prd_lock_counter);
        ret = OMPI_ERROR;
        goto error_exit;
    }

    /* submission part */
    for (i=data->prd_first_active_req; i<data->prd_last_active_req; i++) {
        sqe = io_uring_get_sqe(&reg->ring);
        if (!sqe) {
            /* There is always a chance that another file I/O operation has used
            ** up a slot compared to what we were told by the
            ** io_uring_sp_space_left() function. No damage done, we just locked
            ** a larger chunk of the file than absolutely necessary.
            */
            data->prd_last_active_req = i;
            break;
        }
        
        if (FBTL_POSIX_IO_URING_WRITE_FIXED == type) {
            io_uring_prep_write_fixed(sqe, fh->fd,
                                      data->prd_iou.iou_iov[i].iov_base,
                                      data->prd_iou.iou_iov[i].iov_len,
                                      data->prd_iou.iou_offset[i],
                                      buf_index);
        } else if (FBTL_POSIX_IO_URING_READ_FIXED == type) {
            io_uring_prep_read_fixed(sqe, fh->fd,
                                     data->prd_iou.iou_iov[i].iov_base,
                                     data->prd_iou.iou_iov[i].iov_len,
                                     data->prd_iou.iou_offset[i],
                                     buf_index);
        }
        fbtl_posix_io_uring_rident_t *rident = (fbtl_posix_io_uring_rident_t *) malloc (sizeof(fbtl_posix_io_uring_rident_t));
        if (NULL == rident) {
            opal_output(1, "mca_fbtl_posix_iouring_post_fixed: error in mca_fbtl_posix_lock() "
                        "error ret=%d %s", ret, strerror(errno));
            mca_fbtl_posix_unlock ( &data->prd_lock, data->prd_fh, &data->prd_lock_counter);
            /* TODO: do we need to cancel operations that have already been posted? */
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto error_exit;
        }
        rident->request = request;
        rident->index   = i;
        io_uring_sqe_set_data(sqe, (void*)rident);
        io_uring_submit(&reg->ring);
    }

    req->req_data        = data;
    req->req_progress_fn = mca_fbtl_posix_progress;
    req->req_free_fn     = mca_fbtl_posix_request_free;
#endif

    return OMPI_SUCCESS;

 error_exit:
    if (NULL != data->prd_iou.iou_iov){
        free(data->prd_iou.iou_iov);
    }
    if (NULL != data->prd_iou.iou_offset) {
        free(data->prd_iou.iou_offset);
    }
    if (NULL != data) {
        free (data);
    }
    
    return OMPI_ERROR;
}
