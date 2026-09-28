export interface ReconciliationCursorState {
  cursor: string | null;
  updatedAt: Date;
}

export interface ReconciliationCursorPort {
  load(): Promise<ReconciliationCursorState | null>;
  save(cursor: string | null): Promise<void>;
}
