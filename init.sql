CREATE DATABASE "Payment";

\connect "Payment";

CREATE TABLE "CashFlow" (
   "id" SERIAL NOT NULL,
   "operation" VARCHAR,
   "amount" DECIMAL
)

