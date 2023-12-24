/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2021 University of Houston. All rights reserved.
 * Copyright (c) 2018      Cisco Systems, Inc.  All rights reserved
 * Copyright (c) 2018      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2022      IBM Corporation. All rights reserved
 * Copyright (c) 2024      Advanced Micro Devices, Inc. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * These symbols are in a file by themselves to provide nice linker
 * semantics. Since linkers generally pull in symbols by object fules,
 * keeping these symbols as the only symbols in this file prevents
 * utility programs such as "ompi_info" from having to import entire
 * modules just to query their version and parameters
 */

#include "ompi_config.h"
#include "mpi.h"

#include <unistd.h>
#include <sys/uio.h>

int ompi_fbtl_posix_max_prd_active_reqs=2048;

#include "ompi/mca/fbtl/fbtl.h"
#include "ompi/mca/fbtl/posix/fbtl_posix.h"

#define FBTL_POSIX_LOCK(_lock, _fh, _type, _off, _len, _counter){    \
 int _ret = mca_fbtl_posix_lock(_lock, _fh, _type, _off, _len,       \
                                OMPIO_LOCK_ENTIRE_REGION, _counter); \
 if (0 < _ret) {                                                     \
    opal_output(1, "error in mca_fbtl_posix_lock() %d", _ret);       \
    /* Just in case some part of the lock actually succeeded. */     \
    mca_fbtl_posix_unlock(_lock, _fh, _counter);                     \
    return false;                                                    \
}}

#define FBTL_POSIX_ERR_UNLOCK(_lock, _fh, _counter, _msg) {          \
    opal_output(1, " error in %s", _msg);                            \
    mca_fbtl_posix_unlock (_lock, _fh, _counter);                    \
 }

/*
 * *******************************************************************
 * ************************ actions structure ************************
 * *******************************************************************
 */
static mca_fbtl_base_module_1_0_0_t posix =  {
    mca_fbtl_posix_module_init,     /* initialise after being selected */
    mca_fbtl_posix_module_finalize, /* close a module on a communicator */
    mca_fbtl_posix_preadv,          /* blocking read */
#if defined (FBTL_POSIX_HAVE_AIO)
    mca_fbtl_posix_ipreadv,         /* non-blocking read*/
#else
    NULL,                           /* non-blocking read */
#endif
    mca_fbtl_posix_pwritev,         /* blocking write */
#if defined (FBTL_POSIX_HAVE_AIO)
    mca_fbtl_posix_ipwritev,        /* non-blocking write */
    mca_fbtl_posix_progress,        /* module specific progress */
    mca_fbtl_posix_request_free,    /* free module specific data items on the request */
#else
    NULL,                           /* non-blocking write */
    NULL,                           /* module specific progress */
    NULL,                           /* free module specific data items on the request */
#endif
    mca_fbtl_posix_check_atomicity, /* check whether atomicity is supported on this fs */
#if FBTL_POSIX_HAVE_IO_URING
    mca_fbtl_posix_register_buffers,      /* register bufferes */
    mca_fbtl_posix_unregister_all_buffers /* unregister all buffers */
#else
    NULL,                                 /* register buffers */
    NULL                                  /* unregister all buffers */
#endif
};
/*
 * *******************************************************************
 * ************************* structure ends **************************
 * *******************************************************************
 */

int mca_fbtl_posix_component_init_query(bool enable_progress_threads,
                                      bool enable_mpi_threads) {
    /* Nothing to do */

   return OMPI_SUCCESS;
}

struct mca_fbtl_base_module_1_0_0_t *
mca_fbtl_posix_component_file_query (ompio_file_t *fh, int *priority) {
   *priority = mca_fbtl_posix_priority;

   if (UFS == fh->f_fstype) {
       if (*priority < 50) {
           *priority = 50;
       }
   }

   return &posix;
}

int mca_fbtl_posix_component_file_unquery (ompio_file_t *file) {
   /* This function might be needed for some purposes later. for now it
    * does not have anything to do since there are no steps which need
    * to be undone if this module is not selected */

   return OMPI_SUCCESS;
}

int mca_fbtl_posix_module_init (ompio_file_t *file) {

#if defined (FBTL_POSIX_HAVE_AIO)
    long val = sysconf(_SC_AIO_MAX);
    if ( -1 != val ) {
        ompi_fbtl_posix_max_prd_active_reqs = (int)val;
    }
#endif
#if FBTL_POSIX_HAVE_IO_URING
    mca_fbtl_posix_io_uring_regmem_t *reg = &mca_fbtl_posix_io_uring_data;;
    static bool io_uring_is_init = false;

    if (!io_uring_is_init) {
        int ret = io_uring_queue_init(mca_fbtl_posix_queue_size, &reg->ring, 0);
        if (ret < 0) {
            opal_output(1, "mca_fbtl_posix_module_init: error in io_uring_queue_init");
            return OPAL_ERROR;
        }

        reg->maxelem = 16;
        reg->nelem   = 0;
        reg->iov     = (struct iovec*) malloc (reg->maxelem * sizeof(struct iovec*));
        if (NULL == reg) {
            opal_output(1, "mca_fbtl_posix_module_init: could not allocate memory");
            return OPAL_ERROR;
        }
        io_uring_is_init = true;
    }
#endif

    return OMPI_SUCCESS;
}


int mca_fbtl_posix_module_finalize (ompio_file_t *file)
{
    return OMPI_SUCCESS;
}

static bool mca_fbtl_posix_aio_progress ( mca_ompio_request_t *req)
{
    bool ret=false;
#if defined (FBTL_POSIX_HAVE_AIO)
    int i=0, lcount=0;
    mca_fbtl_posix_request_data_t *data=(mca_fbtl_posix_request_data_t *)req->req_data;
    off_t start_offset, end_offset, total_length;
    
    for (i=data->prd_first_active_req; i < data->prd_last_active_req; i++) {
        if (EINPROGRESS == data->prd_aio.aio_req_status[i]) {
            data->prd_aio.aio_req_status[i] = aio_error (&data->prd_aio.aio_reqs[i]);
            if (0 == data->prd_aio.aio_req_status[i]){
                /* assuming right now that aio_return will return
                ** the number of bytes written/read and not an error code,
                ** since aio_error should have returned an error in that
                ** case and not 0 ( which means request is complete)
                */
                ssize_t ret2 = aio_return (&data->prd_aio.aio_reqs[i]);
                data->prd_total_len += ret2;
                if ( data->prd_aio.aio_reqs[i].aio_nbytes != (size_t)ret2 ) {
                    /* Partial completion */
                    data->prd_aio.aio_reqs[i].aio_offset += ret2;
                    data->prd_aio.aio_reqs[i].aio_buf     = (char*)data->prd_aio.aio_reqs[i].aio_buf + ret2;
                    data->prd_aio.aio_reqs[i].aio_nbytes -= ret2;
                    data->prd_aio.aio_reqs[i].aio_reqprio = 0;
                    data->prd_aio.aio_reqs[i].aio_sigevent.sigev_notify = SIGEV_NONE;
                    data->prd_aio.aio_req_status[i]       = EINPROGRESS;
                    start_offset = data->prd_aio.aio_reqs[i].aio_offset;
                    total_length = data->prd_aio.aio_reqs[i].aio_nbytes;
                    /* release previous lock */
                    mca_fbtl_posix_unlock ( &data->prd_lock, data->prd_fh, &data->prd_lock_counter );
                    
                    if (data->prd_req_type == FBTL_POSIX_AIO_WRITE) {
                        FBTL_POSIX_LOCK(&data->prd_lock, data->prd_fh, F_WRLCK, start_offset, total_length,
                                        &data->prd_lock_counter);
                        if (-1 == aio_write(&data->prd_aio.aio_reqs[i])) {
                            FBTL_POSIX_ERR_UNLOCK (&data->prd_lock, data->prd_fh, &data->prd_lock_counter,
                                                   "aio_write");
                            return false;
                        }
                    }
                    else if (data->prd_req_type == FBTL_POSIX_AIO_READ) {
                        FBTL_POSIX_LOCK(&data->prd_lock, data->prd_fh, F_RDLCK, start_offset, total_length,
                                        &data->prd_lock_counter);
                        if (-1 == aio_read(&data->prd_aio.aio_reqs[i])) {
                            FBTL_POSIX_ERR_UNLOCK (&data->prd_lock, data->prd_fh, &data->prd_lock_counter,
                                                   "aio_read");
                            return false;
                        }
                    }
                }
                else {
                    data->prd_open_reqs--;
                    lcount++;
                }
            }
            else if (EINPROGRESS == data->prd_aio.aio_req_status[i]){
                /* not yet done */
                continue;
            }
            else {
                /* an error occurred. Mark the request done, but
                   set an error code in the status */
                req->req_ompi.req_status.MPI_ERROR = OMPI_ERROR;
                req->req_ompi.req_status._ucount = data->prd_total_len;
                ret = true;
                break;
            }
        }
        else {
            lcount++;
        }
    }
#if 0
    printf("lcount=%d open_reqs=%d\n", lcount, data->prd_open_reqs );
#endif
    if ( (lcount == data->prd_req_chunks) && (0 != data->prd_open_reqs )) {
        /* release the lock of the previous operations */
        mca_fbtl_posix_unlock ( &data->prd_lock, data->prd_fh, &data->prd_lock_counter );
        
        /* post the next batch of operations */
        data->prd_first_active_req = data->prd_last_active_req;
        if ( (data->prd_req_count-data->prd_last_active_req) > data->prd_req_chunks ) {
            data->prd_last_active_req += data->prd_req_chunks;
        }
        else {
            data->prd_last_active_req = data->prd_req_count;
        }

        start_offset = data->prd_aio.aio_reqs[data->prd_first_active_req].aio_offset;
        end_offset   = data->prd_aio.aio_reqs[data->prd_last_active_req-1].aio_offset +
                       data->prd_aio.aio_reqs[data->prd_last_active_req-1].aio_nbytes;
        total_length = (end_offset - start_offset);

        FBTL_POSIX_LOCK(&data->prd_lock, data->prd_fh,
                        FBTL_POSIX_AIO_READ == data->prd_req_type ? F_RDLCK : F_WRLCK,
                        start_offset, total_length, &data->prd_lock_counter);
        
        for ( i=data->prd_first_active_req; i< data->prd_last_active_req; i++ ) {
            if (FBTL_POSIX_AIO_READ == data->prd_req_type) {
                if (-1 == aio_read(&data->prd_aio.aio_reqs[i])) {
                    FBTL_POSIX_ERR_UNLOCK(&data->prd_lock, data->prd_fh, &data->prd_lock_counter,
                                          "aio_read");
                    return false;
                }
            }
            else if (FBTL_POSIX_AIO_WRITE == data->prd_req_type) {
                if (-1 == aio_write(&data->prd_aio.aio_reqs[i])) {
                    FBTL_POSIX_ERR_UNLOCK (&data->prd_lock, data->prd_fh, &data->prd_lock_counter,
                                           "aio_write");
                    return false;
                }
            }
        }
#if 0
        printf("posting new batch: first=%d last=%d\n", data->prd_first_active_req, data->prd_last_active_req );
#endif
    }

    if ( 0 == data->prd_open_reqs ) {
        /* all pending operations are finished for this request */
        req->req_ompi.req_status.MPI_ERROR = OMPI_SUCCESS;
        req->req_ompi.req_status._ucount = data->prd_total_len;
        mca_fbtl_posix_unlock ( &data->prd_lock, data->prd_fh, &data->prd_lock_counter );

        if ( data->prd_fh->f_atomicity ) {
            mca_fbtl_posix_unlock ( &data->prd_lock, data->prd_fh, &data->prd_lock_counter );
        }

        ret = true;
    }
#endif
    return ret;
}

static bool mca_fbtl_posix_iouring_progress (mca_ompio_request_t *req)
{
    bool result = false;
#if FBTL_POSIX_HAVE_IO_URING
    mca_fbtl_posix_request_data_t *data=(mca_fbtl_posix_request_data_t *)req->req_data;
    mca_ompio_request_t *tmpreq;
    fbtl_posix_io_uring_rident_t *tmprident;
    int tmpindex;
    mca_fbtl_posix_request_data_t *tmpreq_data;
    struct io_uring_cqe *cqe;

    int ret = io_uring_peek_cqe(data->prd_iou.iou_ring, &cqe);
    if (ret < 0) {
        /* No entry completed. Includes -EAGAIN */
        goto exit;
    }
    io_uring_cqe_seen(data->prd_iou.iou_ring, cqe);

    /* Update the status of the request associate with this
    ** completion queue entry.
    ** Note: this might be a different request than the one passed
    **       to this function invocation
    */
    tmprident   = (fbtl_posix_io_uring_rident_t *) io_uring_cqe_get_data(cqe);
    tmpreq      = (mca_ompio_request_t *) tmprident->request;
    tmpindex    = tmprident->index;
    tmpreq_data = (mca_fbtl_posix_request_data_t *)tmpreq->req_data;
    if (cqe->res < 0) {
        /* An error has occured.  */
        tmpreq_data->prd_iou.iou_req_status   = -1;
        tmpreq->req_ompi.req_status._ucount   = tmpreq_data->prd_total_len;
        tmpreq->req_ompi.req_status.MPI_ERROR = OMPI_ERROR;

        mca_fbtl_posix_unlock (&tmpreq_data->prd_lock, tmpreq_data->prd_fh,
                               &tmpreq_data->prd_lock_counter);
    } else {
        tmpreq_data->prd_total_len += cqe->res;

        if ((size_t)cqe->res < tmpreq_data->prd_iou.iou_iov[tmpindex].iov_len) {
            /* Partial completion. Recalculate parameters and resubmit */
            struct io_uring_sqe *sqe;
            int buf_index;
	    char **base   = (char**)&tmpreq_data->prd_iou.iou_iov[tmpindex].iov_base;
	    size_t *len   = &tmpreq_data->prd_iou.iou_iov[tmpindex].iov_len;
	    off_t *offset = &tmpreq_data->prd_iou.iou_offset[tmpindex];

            sqe = io_uring_get_sqe(tmpreq_data->prd_iou.iou_ring);
            assert (sqe);

            *base   = *base + cqe->res;
            *len    = *len - cqe->res;
            *offset = *offset + (off_t)cqe->res;
            buf_index = mca_fbtl_posix_get_registration_id(tmpreq_data->prd_fh, *base, *len);
            if (FBTL_POSIX_IO_URING_WRITE_FIXED == tmpreq_data->prd_req_type) {
                io_uring_prep_write_fixed(sqe, tmpreq_data->prd_fh->fd, *base, *len, *offset,
					  buf_index);
            } else if (FBTL_POSIX_IO_URING_READ_FIXED == tmpreq_data->prd_req_type) {
                io_uring_prep_read_fixed(sqe, tmpreq_data->prd_fh->fd, *base, *len, *offset,
					 buf_index);
            }
            io_uring_sqe_set_data(sqe, (void*)tmprident);
            io_uring_submit(tmpreq_data->prd_iou.iou_ring);
        } else {
            /* This part of the request is done. */
            tmpreq_data->prd_iou.iou_req_status   = 1;
            tmpreq->req_ompi.req_status._ucount   = tmpreq_data->prd_total_len;
            tmpreq->req_ompi.req_status.MPI_ERROR = OMPI_SUCCESS;

            mca_fbtl_posix_unlock (&tmpreq_data->prd_lock, tmpreq_data->prd_fh,
                                   &tmpreq_data->prd_lock_counter);
        }
    }
 exit:
    /* Check whether we can post the next batch of operations for the 
    ** request passed to this invocation
    */
    
    /* Check whether this request is complete */
    if (1 == data->prd_iou.iou_req_status) {
        result = true;
    }
#endif
    return result;
}

bool mca_fbtl_posix_progress ( mca_ompio_request_t *req)
{
     mca_fbtl_posix_request_data_t *data=(mca_fbtl_posix_request_data_t *)req->req_data;
#if defined (FBTL_POSIX_HAVE_AIO)
     if (FBTL_POSIX_AIO_READ  == data->prd_req_type ||
         FBTL_POSIX_AIO_WRITE == data->prd_req_type) {
         return mca_fbtl_posix_aio_progress(req);
     }
#endif
#if FBTL_POSIX_HAVE_IO_URING
     if (FBTL_POSIX_IO_URING_READ_FIXED  == data->prd_req_type ||
         FBTL_POSIX_IO_URING_WRITE_FIXED == data->prd_req_type ||
         FBTL_POSIX_IO_URING_READ        == data->prd_req_type ||
         FBTL_POSIX_IO_URING_WRITE       == data->prd_req_type ) {
         return mca_fbtl_posix_iouring_progress(req);
     }
#endif
     return false;
}

void mca_fbtl_posix_request_free ( mca_ompio_request_t *req)
{
    /* Free the fbtl specific data structures */
    mca_fbtl_posix_request_data_t *data=(mca_fbtl_posix_request_data_t *)req->req_data;

    if (NULL != data) {
#if defined (FBTL_POSIX_HAVE_AIO)
        if (FBTL_POSIX_AIO_READ  == data->prd_req_type ||
            FBTL_POSIX_AIO_WRITE == data->prd_req_type) {
            if (NULL != data->prd_aio.aio_reqs) {
                free (data->prd_aio.aio_reqs);
            }
            if (NULL != data->prd_aio.aio_req_status) {
                free (data->prd_aio.aio_req_status);
            }
        }
#endif
#if FBTL_POSIX_HAVE_IO_URING
        if (FBTL_POSIX_IO_URING_READ_FIXED  == data->prd_req_type ||
            FBTL_POSIX_IO_URING_WRITE_FIXED == data->prd_req_type ||
            FBTL_POSIX_IO_URING_READ        == data->prd_req_type ||
            FBTL_POSIX_IO_URING_WRITE       == data->prd_req_type ) {
            if (NULL != data->prd_iou.iou_iov) {
                free (data->prd_iou.iou_iov);
            }
        }
#endif
    }
    free (data);
    req->req_data = NULL;

    return;
}

bool mca_fbtl_posix_check_atomicity ( ompio_file_t *file)
{    
    struct flock lock;
    
    lock.l_type   = F_WRLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start  = 0;
    lock.l_len    = 0;
    lock.l_pid    = 0;
    
    if (fcntl(file->fd, F_GETLK, &lock) < 0)
    {
#ifdef VERBOSE
        printf("Failed to get lock info for '%s': %s\n", filename, strerror(errno));
#endif
        return false;
    }

#ifdef VERBOSE
    printf("Lock would have worked, l_type=%d\n", (int)lock.l_type);
#endif
    return true;
}
