const jwt = require("jsonwebtoken");
const { handlers } = require("../utilities/handlers");

const secretKey = process.env.JWT_SECRET;
const tokenExpirationTime = process.env.TOKEN_EXPIRATION;

const secureEnv = process.env.NODE_ENV;
const sameSite = process.env.SAME_SITE;
const maxAge = process.env.MAX_AGE;

const generateToken = ({ _id, res }) => {
  try {
    const token = jwt.sign({ _id }, secretKey, {
      expiresIn: tokenExpirationTime
    });

    res.cookie("authorization", token, {
      httpOnly: true,
      secure: secureEnv === "production",
      sameSite: sameSite,
      maxAge: maxAge
    });

    return token;
  } catch (error) {
    handlers.logger.error({ message: error });
  }
};

module.exports = generateToken;
