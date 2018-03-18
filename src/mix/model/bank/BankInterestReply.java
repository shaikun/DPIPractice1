package mix.model.bank;

import mix.model.ISerializable;
import mix.model.loan.LoanRequest;

/**
 * This class stores information about the bank reply
 * to a loan request of the specific client
 */
public class BankInterestReply implements ISerializable {

    private double interest; // the loan interest
    private String bankId; // the nunique quote Id
    private LoanRequest loanRequest;

    public BankInterestReply() {
        this.interest = 0;
        this.bankId = "";
    }

    public BankInterestReply(double interest, String quoteId) {
        this.interest = interest;
        this.bankId = quoteId;
    }

    public double getInterest() {
        return interest;
    }

    public void setInterest(double interest) {
        this.interest = interest;
    }

    public String getQuoteId() {
        return bankId;
    }

    public void setQuoteId(String quoteId) {
        this.bankId = quoteId;
    }

    public String toString() {
        return "quote=" + this.bankId + " interest=" + this.interest;
    }

    public LoanRequest getLoanRequest() {
        return loanRequest;
    }

    public void setLoanRequest(LoanRequest loanRequest) {
        this.loanRequest = loanRequest;
    }
}
