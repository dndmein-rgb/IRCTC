import sgMail from '@sendgrid/mail';
import 'dotenv/config';
import { config } from '../config';

const apiKey = process.env.SENDGRID_API_KEY;

if (!apiKey) {
    throw new Error('FATAL: SENDGRID_API_KEY environment variable is not defined.');
}

sgMail.setApiKey(apiKey);

const minutes = Number(config.OTP_TTL || 300) / 60;

export async function sendOtpEmail(email, otp) {
    const msg = {
        to: email,
        from: `${config.MAIL_SEND}`,
        subject: 'Your verification code',
        html: `
        <div style="
            font-family: Arial, sans-serif;
            max-width: 420px;
            margin: auto;
            padding: 20px;
            border: 1px solid #e0e0e0;
            border-radius: 8px;">
            <h2 style="color: #333333; text-align: center;">Verification Code</h2>
            <p style="color: #555555; font-size: 14px;">Your OTP for verification is:</p>
            <h1 style="color: #4CAF50; letter-spacing: 4px; text-align: center;">${otp}</h1>
            <p style="color: #777777; font-size: 12px; margin-top: 20px;">This code will expire in ${minutes} minutes.</p>
        </div>`
    };

    try {
        await sgMail.send(msg);
    } catch (error) {
        console.error("Error sending OTP email:", error);
        throw error;
    }
}

export async function verifyOtpEmail(email) {
    const msg = {
        to: email,
        from: `${config.MAIL_SEND}`,
        subject: 'Email Verification Successful',
        html: `
        <div style="
            font-family: Arial, sans-serif;
            max-width: 420px;
            margin: auto;
            padding: 20px;
            border: 1px solid #e0e0e0;
            border-radius: 8px;">
            <h2 style="color: #4CAF50; text-align: center;">Verification Successful</h2>
            <p style="color: #555555; font-size: 14px;">Your email address has been successfully verified.</p>
            <p style="color: #777777; font-size: 12px; margin-top: 20px;">You can now log in and safely access your account.</p>
        </div>`
    };

    try {
        await sgMail.send(msg);
    } catch (error) {
        console.error("Error sending verification confirmation email:", error);
        throw error;
    }
}