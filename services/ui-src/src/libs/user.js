import { Auth } from "aws-amplify";
import config from "../config";

const userKey = "userKey";

export async function updateCurrentUserAttributes(userAttributes) {
  const localLogin = config.LOCAL_LOGIN === "true";
  if (localLogin) {
    return updateLocalCurrentUserAttributes(userAttributes);
  } else {
    let user = await Auth.currentAuthenticatedUser();
    return Auth.updateUserAttributes(user, userAttributes);
  }
}

export function updateLocalCurrentUserAttributes(userAttributes) {
  const store = window.localStorage;
  var info = JSON.parse(store.getItem(userKey));
  info.attributes = { ...info.attributes, ...userAttributes };
  store.setItem(userKey, JSON.stringify(info));
}

export async function currentUserInfo() {
  const localLogin = config.LOCAL_LOGIN === "true";

  if (localLogin) {
    return getLocalUserInfo();
  } else {
    return Auth.currentUserInfo();
  }
}

export function getLocalUserInfo() {
  const store = window.localStorage;

  const info = JSON.parse(store.getItem(userKey));

  return info;
}

export async function loginLocalUser(userInfo) {
  const store = window.localStorage;

  store.setItem(userKey, JSON.stringify(userInfo));
}
