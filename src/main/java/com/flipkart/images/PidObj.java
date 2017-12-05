package com.flipkart.images;

class PidObj {

    String pid;
    String vertical;
    String imageUrl;

    public PidObj(String pid, String vertical, String imageUrl) {
        this.pid = pid;
        this.vertical = vertical;
        this.imageUrl = imageUrl;
    }

    public static PidObj create(String[] args) {
        return new PidObj(args[0], args[1], args[2]);
    }

    public String getBaseImageUrl() {
        return "/image" + imageUrl.replaceAll("http://img.fkcdn.com/image/[0-9]*/[0-9]*", "");
    }

    @Override
    public String toString() {
        return "PidObj{" +
                "pid='" + pid + '\'' +
                ", vertical='" + vertical + '\'' +
                ", imageUrl='" + imageUrl + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PidObj pidObj = (PidObj) o;

        if (pid != null ? !pid.equals(pidObj.pid) : pidObj.pid != null) return false;
        if (vertical != null ? !vertical.equals(pidObj.vertical) : pidObj.vertical != null) return false;
        return imageUrl != null ? imageUrl.equals(pidObj.imageUrl) : pidObj.imageUrl == null;
    }

    @Override
    public int hashCode() {
        int result = pid != null ? pid.hashCode() : 0;
        result = 31 * result + (vertical != null ? vertical.hashCode() : 0);
        result = 31 * result + (imageUrl != null ? imageUrl.hashCode() : 0);
        return result;
    }
}
