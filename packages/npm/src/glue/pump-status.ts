// Bits of a dialed transport's status word (an int32 C++ mallocs per dial). The pump thread ORs them in and
// notifies; the main glue watches the word and frees it once RELEASED is set.
export const PUMP_OPENED = 1;
export const PUMP_CLOSED = 2;
// The pump has freed the ring and will never touch the ring or this word again.
export const PUMP_RELEASED = 4;
