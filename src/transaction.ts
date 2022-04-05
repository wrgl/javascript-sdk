export type CreateTransactionRequest = {
  id: string;
  begin: Date;
  end: Date;
  status: string;
};

export type CreateTransactionRawRequest = Omit<
  CreateTransactionRequest,
  "begin" | "end"
> & {
  begin: string;
  end: string;
};

export const createTransactionRequestPayload = (
  obj: CreateTransactionRequest
): CreateTransactionRawRequest => {
  return {
    ...obj,
    begin: obj.begin.toISOString(),
    end: obj.end.toISOString(),
  };
};

export type CreateTransactionResponse = {
  id: string;
};

export type TxBranch = {
  name: string;
  currentSum: string;
  newSum: string;
};

export type GetTransactionResponse = {
  status: string;
  begin: Date;
  end: Date;
  branches: TxBranch[];
};

export type GetTransactionRawResponse = Omit<
  GetTransactionResponse,
  "begin" | "end"
> & {
  begin: string;
  end: string;
};

export const getTransactionResponse = (
  obj: GetTransactionRawResponse
): GetTransactionResponse => {
  return {
    ...obj,
    begin: new Date(obj.begin),
    end: new Date(obj.end),
  };
};
