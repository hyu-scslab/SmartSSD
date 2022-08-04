/*-------------------------------------------------------------------------
 *
 * vi_writer.h
 *	  Defines operations related to version index modification.
 *
 *
 * Portions Copyright (c) 1996-2019, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/vi_writer.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VI_WRITER_H
#define VI_WRITER_H

extern void ViGetFreeSlot(Oid rel_id, Oid seg_id,
						  ViPageId *page_id, ViPageOffset *offset,
						  TransactionId xmin);

extern void ViToastViCtid(ViCtid vi_ctid,
						  TransactionId xmin, TransactionId xmax,
						  uint16 infomask, SegmentOffset offset,
						  ItemLength len, uint8 h_len,
						  bits8 *bits, int numbits);

extern void ViCopyViCtidIntoSlot(ViBuffer frame_id, ViPageOffset offset,
								 ViCtid vi_ctid);

extern ViPageOffset ViExportCtidChunk(ViBuffer frame_id, CtidChunk ctid_chunk,
									  bool *is_done);

extern uint32 ViPrescreenAndExportIndexPage(int fd, off_t *offset,
											ViBuffer frame_id,
											Snapshot snapshot);

extern void ViUpdateXminInternal(ViBuffer frame_id, ViPageOffset offset,
								 TransactionId xmin);

extern void ViUpdateXmaxInternal(ViBuffer frame_id, ViPageOffset offset,
								 TransactionId xmax);

extern void ViUpdateSegmentOffsetInternal(ViBuffer frame_id,
										  ViPageOffset offset,
										  SegmentOffset new_seg_offset);

extern void ViUpdateHintBitsInternal(ViBuffer frame_id, ViPageOffset offset,
									 uint16 infomask);

extern void ViSetHintBitsInternal(ViBuffer frame_id, ViPageOffset offset,
								  uint16 infomask);

#endif /* VI_WRITER_H */
