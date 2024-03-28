CREATE TABLE [PMS_Controller].[Transactions].[RazorpayLog] (
        id BIGINT NOT NULL IDENTITY(1,1),
        org_id BIGINT NULL,
        razorpay_order_id NVARCHAR(500) NULL,
        razorpay_payment_id NVARCHAR(500) NULL,
        razorpay_signature NVARCHAR(500) NULL,
        razorpay_order NVARCHAR(max) NULL,
        razorpay_payment NVARCHAR(max) NULL,
        PRIMARY KEY (id)
)